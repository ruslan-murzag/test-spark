from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, udf, when, lit
from pyspark.sql.types import StringType
import requests
import urllib.parse
import geohash2

# Here must be API KEY
API_KEY = ''



spark = SparkSession.builder \
    .appName("SparkPracticeFull") \
    .master("local[*]") \
    .getOrCreate()

# Your Dataframes and result
restaurants_path = "data/restaurants"
weather_path = "data/weather"
output_path = "data/output/enriched_final"

# Read restaurants
restaurants_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(restaurants_path)

# Get no null data and nul data
no_null_df = restaurants_df.filter(col("lat").isNotNull() & col("lng").isNotNull())
null_df = restaurants_df.filter(col("lat").isNull() | col("lng").isNull())

null_rows = null_df.select("id", "city", "country").collect()


# Function to request API
def get_coordinates(city, country):
    query = f"{city},{country}"
    url = f"https://api.opencagedata.com/geocode/v1/json?q={urllib.parse.quote(query)}&key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        results = data.get("results", [])
        if results:
            geometry = results[0].get("geometry", {})
            lat = geometry.get("lat", None)
            lng = geometry.get("lng", None)
            return lat, lng
    return None, None


# New coords for null
updated_coords = []
for row in null_rows:
    lat_val, lng_val = get_coordinates(row.city, row.country)
    if lat_val is None or lng_val is None:
        lat_val, lng_val = 0.0, 0.0
    updated_coords.append((row.id, lat_val, lng_val))

# updated coordinates
updated_coords_df = spark.createDataFrame(updated_coords, ["id", "lat_new", "lng_new"])

# join all dataframes to get no null df
null_fixed_df = null_df.join(updated_coords_df, on="id", how="left") \
    .withColumn("final_lat", coalesce(col("lat"), col("lat_new"))) \
    .withColumn("final_lng", coalesce(col("lng"), col("lng_new"))) \
    .drop("lat_new", "lng_new")

# null_fixed_df = null_fixed_df \
#     .withColumn("final_lat", when(col("final_lat").isNull(), lit(0.0)).otherwise(col("final_lat"))) \
#     .withColumn("final_lng", when(col("final_lng").isNull(), lit(0.0)).otherwise(col("final_lng")))


# Create new column in order to union the data from api
no_null_df = no_null_df \
    .withColumn("final_lat", col("lat")) \
    .withColumn("final_lng", col("lng"))

fixed_restaurants_df = no_null_df.unionByName(null_fixed_df.select(no_null_df.columns))


# function generate hasg
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        return geohash2.encode(lat, lng, precision=4)
    return None


geohash_udf = udf(generate_geohash, StringType())

restaurants_with_geohash = fixed_restaurants_df \
    .withColumn("geohash", geohash_udf(col("final_lat"), col("final_lng")))

weather_df = spark.read.parquet(weather_path)

weather_with_geohash = weather_df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

weather_unique = weather_with_geohash.dropDuplicates(["geohash"])

weather_renamed = weather_unique \
    .withColumnRenamed("lat", "w_lat") \
    .withColumnRenamed("lng", "w_lng") \
    .withColumnRenamed("avg_tmpr_f", "w_avg_tmpr_f") \
    .withColumnRenamed("avg_tmpr_c", "w_avg_tmpr_c") \
    .withColumnRenamed("wthr_date", "w_wthr_date")

enriched_df = restaurants_with_geohash.join(weather_renamed, on="geohash", how="left")

# Result
enriched_df.write \
    .partitionBy("geohash") \
    .mode("overwrite") \
    .parquet(output_path)

spark.stop()