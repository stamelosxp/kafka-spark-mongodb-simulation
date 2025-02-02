from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pymongo.errors import ConnectionFailure
from pymongo import MongoClient

def createDatabase(dbName):
    try:
        # Connect to MongoDB server
        client = MongoClient('mongodb://127.0.0.1:27017/')

        # Check if the database exists
        if "big_data" in client.list_database_names():
            print(f"Database already exists.")
        else:
            # Create the database by inserting a dummy document into a dummy collection
            new_db = client["big_data"]
            processed_data = new_db["db.processed_data"]
            raw_data = new_db["raw_data"]
            print(f"Database created successfully.")

    except ConnectionFailure:
        print("Failed to connect to MongoDB server.")
    finally:
        client.close()

def sparkConn():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('KafkaSparkProcessing') \
            .master('spark://bigdata-vm:7077') \
            .config('spark.jars.packages', "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/big_data.big_data") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .getOrCreate()
    except Exception as e:
        print(e)
    return s_conn

def kafkaConn(spark_conn, nameTopic):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', nameTopic) \
            .option('startingOffsets', 'earliest') \
            .load()
    except Exception as e:
        print('Error connecting to Kafka')
        print(e)
    return spark_df

def rawToMongo(batch_df):
    batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://127.0.0.1/big_data.raw_data") \
        .save()

def processedToMongo(batch_df):
    result_df = batch_df.groupBy("time", "link").agg(
        expr("count(*) as vcount"),
        expr("avg(speed) as vspeed")
    )
    result_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", "mongodb://127.0.0.1/big_data.processed_data") \
        .save()

def process_batch(batch_df,batch_id):
    batch_df.show()
    # Write raw data to MongoDB
    rawToMongo(batch_df)
    # Process data and write to MongoDB
    processedToMongo(batch_df)

if __name__ == "__main__":
    dbName = 'big_data'
    createDatabase(dbName)
    nameTopic = 'vehicle_positions'
    spark_conn = sparkConn()

    if spark_conn:
        # Connect to Kafka with Spark connection
        kafka_df = kafkaConn(spark_conn, nameTopic)
        if kafka_df:
            # Define the schema for the JSON data
            schema = StructType([
                StructField("name", StringType(), True),
                StructField("origin", StringType(), True),
                StructField("destination", StringType(), True),
                StructField("time", StringType(), True),
                StructField("link", StringType(), True),
                StructField("position", FloatType(), True),
                StructField("spacing", FloatType(), True),
                StructField("speed", FloatType(), True)
            ])
            
            # Parse the JSON data and select relevant columns
            parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
                                .select(from_json(col("json_string"), schema).alias("data")) \
                                .select("data.*")
                                
            # Write stream to MongoDB and process the data
            query = parsed_df.writeStream \
                             .outputMode("append") \
                             .foreachBatch(process_batch) \
                             .start()
            
            # Await termination
            query.awaitTermination()
