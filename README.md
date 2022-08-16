# Pyspark-goole_cloud




Dataproc üzerinden cluster yaratıyoruz. 

Ssh ile bu cluster baglanıyoruz

----
zookeeper çalıştırma 
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &

kafka run 
nohup bin/kafka-server-start.sh config/server.properties &

topic oluşturma 
bin/kafka-topics.sh --create --topic dce-test  --bootstrap-server localhost:9092

procuder oluşturma
cd kafka-topics

 bin/kafka-console-producer.sh --topic dce-test --bootstrap-server localhost:9092

consumer oluşturma
bin/kafka-console-consumer.sh --topic dce-test --bootstrap-server localhost:9092
