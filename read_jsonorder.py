

from pyspark.shell import spark
#from read_jsonorder import timestamp
from pyspark.sql.functions import to_timestamp, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

schema = StructType([
        StructField("Timestamtp",IntegerType(),True),
        StructField("event", StringType(), True),
        StructField("messageid", StringType(), True),
         StructField("userid", StringType(), True),
        StructField("orderid", StringType(),True),

        StructField("lineitems", ArrayType(StructType([
        StructField("productid", StringType(),False),
        StructField("quantity", StringType(),False)

    ]))),

    ])

df_with_schema = spark.read.json("/../data/orders.json")
df_with_schema.printSchema()
df_with_schema.show(truncate=False)
#df = spark.read.json("../orders.json")
#df.withColumn("current_timestamp",current_timestamp()).show(truncate=False)
#df1 = df.withColumn("current_date", current_date())
#df1.show()