
from pyspark.sql import SparkSession
from pyspark.sql.functions import  current_timestamp,count

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

appName = "PySpark Example - Read JSON file from GCS"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Create a schema for the dataframe
schema = StructType([
    StructField("RecordNumber",IntegerType(),True),
    StructField("event", StringType(), True),
    StructField("messageid", StringType(), True),
    StructField("userid", StringType(), True),
    StructField("properties", StructType([
        StructField("productid", StringType()),
    ])),
    StructField("context", StructType([
        StructField("source", StringType()),
    ])),

])

# Create data frame
json_file_path = 'gs://product-views/order.json'
df = spark.read.json(json_file_path)
df.withColumn("current_timestamp", current_timestamp()).show(truncate=False)
df.distinct().show()
a=df.select(count('userid')).collect()
print(a)
#print(df.schema)
#df.show()

words = spark.read.format('bigquery').option('table', 'bigquery-public-data:samples.shakespeare') \
  .load()
words.createOrReplaceTempView('words')

# Perform word count.
word_count = spark.sql(
    'SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word')
word_count.show()
word_count.printSchema()

# Saving the data to BigQuery
word_count.write.format('bigquery') \
  .option('table', 'wordcount_dataset.wordcount_output') \
  .save()
