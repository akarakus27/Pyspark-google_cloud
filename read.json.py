
# Read JSON file into dataframe
from pyspark.shell import spark

# Define custom schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


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
#df = spark.read.json("/Users/abdulkadirkarakus/Desktop/Karakus/İstDataAcedemy/hepsiburada/data")
df_with_schema = spark.read.schema(schema) \
        .json("/Users/abdulkadirkarakus/Desktop/Karakus/İstDataAcedemy/hepsiburada/data")
df_with_schema.printSchema()
df_with_schema.show()
