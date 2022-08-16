from pyspark.sql.functions import  current_timestamp
from kafka import KafkaProducer
import json
from pyspark.shell import spark

if __name__ == "__main__":
   "execute onlu if run as a script"

def dataProducer():
   df = spark.read.json("../data/product-views.json")
   df.withColumn("current_timestamp", current_timestamp()).show(truncate=False)
   return df


data = dataProducer()
procuder = KafkaProducer(value_serializer =lambda m: json.dumps(m).encode('utf-8'))
procuder.send('spark-exp', {'context': data[1], 'event': data[2], 'messageid': data[3], 'properties': data[4], 'userid':data[5], 'TimeStamp':data[6]})
print("Success")
procuder.flush()






