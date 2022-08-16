from pyspark.sql.functions import to_timestamp, current_date, current_timestamp
from kafka import KafkaProducer
import json

from pyspark.shell import spark

if __name__ == "__main__":
   "execute onlu if run as a script"
def dataProducer():
   df = spark.read.json(".../data/order.json")
   df.withColumn("current_timestamp", current_timestamp()).show(truncate=False)
   return df


data = dataProducer()
procuder = KafkaProducer(value_serializer =lambda m: json.dumps(m).encode('utf-8'))
procuder.send('spark-exp', {'event': data[0], 'messageid': data[1], 'userid': data[2], 'orderid': data[3], 'lineitems':data[4], 'TimeStamp':data[5],})
print("Success")
procuder.flush()






