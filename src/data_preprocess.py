from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, year, when, month, create_map, lit, date_format
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from itertools import chain

# storage_account_name = ''
# storage_account_key = ''
# container_name = ''
# dbutils.fs.mount(
#     source = '',
#     mount_point = f"/mnt/aviation-data",
#     extra_configs = {}
# )

def preprocess_data(): 
    spark = SparkSession.builder.appName('FlightDelayPrediction').getOrCreate()
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    df = spark.read.parquet("dbfs:/mnt/aviation-data/ontime.*.parquet")
    df = df.withColumn("date_of_flight", date_format(from_unixtime(col("date_of_flight") / 1e9),"yyyy-MM-dd"))
    df = df.withColumn("season", when(month(col("date_of_flight")).isin([12, 1, 2]), "Winter").when(month(col("date_of_flight")).isin([3, 4, 5]), "Spring").when(month(col("date_of_flight")).isin([6, 7, 8]), "Summer").otherwise("Fall"))

    df = df.drop("carrier_id","diverted_airport_1", "diverted_airport_2", "diverted_airport_3", "diverted_airport_4", "diverted_airport_5", "gate_time_diverted_1", "gate_time_diverted_2", "gate_time_diverted_3", "gate_time_diverted_4", "gate_time_diverted_5", "longest_gate_time_diverted_1", "longest_gate_time_diverted_2", "longest_gate_time_diverted_3", "longest_gate_time_diverted_4", "longest_gate_time_diverted_5", "__null_dask_index__")

    indexers = [
        StringIndexer(inputCol=column, outputCol=f"{column}_indexed", handleInvalid="skip")
        for column in ["carrier_code", "departure_airport", "arrival_airport", "season"]
    ]

    feature_columns= [
        "departure_delay", "weather_delay", "carrier_delay", "nas_delay", 
        "security_delay", "late_aircraft_delay", "day_of_week"
    ]

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    pipeline = Pipeline(stages=indexers + [assembler])
    processed_df = pipeline.fit(df).transform(df)

    processed_df.write.format("delta").save("dbfs:/mnt/aviation-data/processed_data.delta")   

if __name__ == "__main__":
     preprocess_data()   
    



