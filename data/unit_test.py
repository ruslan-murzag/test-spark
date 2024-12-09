import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    # init Spark Session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("TestApp") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_no_null_df_copy_coordinates_to_final(spark):
    # DataFrame
    data = [
        (1, "Franchise A", "Restaurant A", "US", "New York", 40.7128, -74.0060),
        (2, "Franchise B", "Restaurant B", "IT", "Milan", 45.4642, 9.1900)
    ]
    columns = ["id", "franchise_name", "restaurant_franchise_id", "country", "city", "lat", "lng"]
    no_null_df = spark.createDataFrame(data, columns)

    # Operation to check
    no_null_df = no_null_df \
        .withColumn("final_lat", col("lat")) \
        .withColumn("final_lng", col("lng"))

    # Collect data to local list, to check
    result = no_null_df.select("id", "lat", "lng", "final_lat", "final_lng").collect()

    # check that final_lat/ final_lng equal to lat/lng
    for row in result:
        assert row.final_lat == row.lat, f"final_lat {row.final_lat} != lat {row.lat}"
        assert row.final_lng == row.lng, f"final_lng {row.final_lng} != lng {row.lng}"