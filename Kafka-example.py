from kafka import KafkaProducer
from kafka.errors import KafkaError
from json import dumps
import json
import random
from pyspark.shell import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


if __name__ == "__main__":
   "execute onlu if run as a script"

   acct_number = [11111111111, 2222222, 33333333, 44444444]
   phone_list =['Iphone', 'Samsung', 'Xiomi']
   iph_model_list = ['XR', '6S', '12', '11', '5S']
   samsung_list = ['A 12', 'J5', 'S20', 'S3']
   xiomi_list = ['NOTE 8', 'MI 5S', 'MI PLUS']
   dev_id_list =['mobil', 'web']


   while 1==1:
      check= 3
      acct_id = random.choice(acct_number)
      dv_id = random.choice(dev_id_list)
      phone_id = random.choice(phone_list)
      if phone_id == 'Iphone':
         model_id = random.choice(iph_model_list)
      elif phone_id == 'Samsung':
         model_id= random.choice(iph_model_list)
      else:
         model_id= random.choice(iph_model_list)



      procuder =KafkaProducer(value_serializer =lambda m: json.dumps(m).encode('utf-8'))
      procuder.send('spark-exp', {'acct_num': acct_id, 'dev_id': dv_id, 'phone': phone_id, 'model': model_id})
      print("Success")
      procuder.flush()






