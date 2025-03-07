
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Current date in format year=xxxx month=xx

current_date = datetime.now()
month = current_date.strftime("%m")
year = current_date.year - 4

# URLs parsing and downloading files

def download_file(url, filename):
    response = requests.get(url)
    with open(filename, "wb") as file:
        file.write(response.content)

url_yellow = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month}.csv.gz'
url_green = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month}.csv.gz'
url_zones = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

download_file(url_yellow, f"yellow_tripdata_{year}-{month}.csv.gz")
download_file(url_green, f"green_tripdata_{year}-{month}.csv.gz")
download_file(url_zones, 'taxi_zone_lookup.csv')

# Postgres connecting

engine = create_engine('postgresql://root:root@localhost:5432/taxi') # Take care of Postgres parameters

# Function for loading data into Postgres and formatting some columns string --> datetime

def process_taxi_data(file_prefix, time_columns):
    df_iter = pd.read_csv(f'{file_prefix}_tripdata_{year}-{month}.csv.gz', iterator=True, chunksize=100000, compression='gzip')
    df = next(df_iter)
    for col in time_columns:
        df[col] = pd.to_datetime(df[col])
    
    table_name = f'{file_prefix}_tripdata_{year}_{month}'
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    for df_chunk in df_iter:
        for col in time_columns:
            df_chunk[col] = pd.to_datetime(df_chunk[col])
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

# Loading tables into Postgres

process_taxi_data("yellow", ["tpep_pickup_datetime", "tpep_dropoff_datetime"])
process_taxi_data("green", ["lpep_pickup_datetime", "lpep_dropoff_datetime"])

# Loading zones data

df_zone = pd.read_csv('taxi_zone_lookup.csv')
df_zone.to_sql(name='zones', con=engine, if_exists='replace', index=False)

# Function of creating new table with joining zone names instead of ZoneID

def create_taxi_zones_table(color, pickup_col, dropoff_col):
    table_name = f"{color}_tripdata_{year}_{month}_zones"
    base_table = f"{color}_tripdata_{year}_{month}"

    create_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        index SERIAL PRIMARY KEY,
        "VendorID" INTEGER,
        "pickup_datetime" TIMESTAMP,
        "dropoff_datetime" TIMESTAMP,
        "passenger_count" INTEGER,
        "total_amount" DOUBLE PRECISION,
        "PULocation" TEXT,
        "DOLocation" TEXT
    );
    """
    with engine.connect() as connection:
        connection.execute(text(create_query))
        connection.commit()
        print(f'Table {table_name} created')
    
    insert_query = f"""
    INSERT INTO {table_name} (
        "VendorID", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "total_amount", "PULocation", "DOLocation"
    )
    SELECT 
        y."VendorID", y.{pickup_col}, y.{dropoff_col},
        y.passenger_count, y.total_amount,
        concat(z1."Zone", ' / ', z1."Borough") AS "PULocation", 
        concat(z2."Zone", ' / ', z2."Borough") AS "DOLocation"
    FROM {base_table} y
    LEFT JOIN zones z1 ON y."PULocationID" = z1."LocationID"
    LEFT JOIN zones z2 ON y."DOLocationID" = z2."LocationID";
    """
    with engine.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {table_name};"))
        connection.execute(text(insert_query))
        connection.commit()
        print(f'Data inserted into {table_name}')

# New tables with zone names

create_taxi_zones_table("yellow", "tpep_pickup_datetime", "tpep_dropoff_datetime")
create_taxi_zones_table("green", "lpep_pickup_datetime", "lpep_dropoff_datetime")

# Spark initialization

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('taxi') \
    .config("spark.pyspark.python", "python") \
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Function for converting pd_df --> spark_df

def load_spark_df(query):
    return spark.createDataFrame(pd.read_sql(query, engine)).repartition(12)

df_yellow_spark = load_spark_df(f"SELECT * FROM yellow_tripdata_{year}_{month}_zones")
df_green_spark = load_spark_df(f"SELECT * FROM green_tripdata_{year}_{month}_zones")

# Creating ranked top-10 on drop-off location within the total revenue

def get_top_zones(df_spark):
    window_spec = Window.orderBy(F.desc("Revenue"))
    return df_spark.groupBy("DOLocation") \
        .agg(F.sum("total_amount").alias("Revenue")) \
        .withColumn("Revenue", F.round("Revenue", 2)) \
        .withColumn("Place", F.row_number().over(window_spec)) \
        .orderBy(F.desc("Revenue")) \
        .limit(10)

df_top_yellow_spark = get_top_zones(df_yellow_spark)
df_top_green_spark = get_top_zones(df_green_spark)

# Converting spark_df --> pd_df 

df_top_yellow_pd = df_top_yellow_spark.toPandas()
df_top_green_pd = df_top_green_spark.toPandas()

# Loading data to Postgres

def save_top_zones(df_pd, table_name):
    df_pd.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    df_pd.to_sql(name=table_name, con=engine, if_exists='append', index=False)

save_top_zones(df_top_yellow_pd, f'yellow_top_10_{year}_{month}')
save_top_zones(df_top_green_pd, f'green_top_10_{year}_{month}')

print ('The job has been done successfully')

spark.stop()
