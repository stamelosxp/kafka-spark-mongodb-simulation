from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .master('spark://bigdata-vm:7077') \
            .config('spark.jars.packages', "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .getOrCreate()
    except Exception as e:
        print(e)
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'test1') \
            .load()
    except Exception as e:
        print('Error connecting to Kafka')
        print(e)
    return spark_df

def process_batch(batch_df, batch_id):
    # Perform any batch-specific processing here
    batch_df.show()  # For example, show the batch data
    # Additional processing logic can be added here, e.g., writing to a database

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()
    if spark_conn:
        # Connect to Kafka with Spark connection
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df:
            # Define the schema for the JSON data
            schema = StructType([
                StructField("name", StringType(), True),
                StructField("origin", StringType(), True),
                StructField("destination", StringType(), True),
                StructField("time", StringType(), True),
                StructField("link", StringType(), True),
                StructField("potition", StringType(), True),
                StructField("spacing", StringType(), True),
                StructField("speed", StringType(), True)
            ])
            
            
            # Parse the JSON data and select relevant columns
            parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
                                .select(from_json(col("json_string"), schema).alias("data")) \
                                .select("data.*")
                                
            # Debugging: Write to console to inspect the raw and parsed data
            query = parsed_df.writeStream \
                             .outputMode("append") \
                             .foreachBatch(process_batch) \
                             .start()
            
            # Await termination
            query.awaitTermination()
