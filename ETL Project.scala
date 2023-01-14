/*ZooKeeper*/
cd zookeeper-3.4.14
bin/zkServer.sh start

/*Starting Kafka*/
cd confluent-4.1.4
nohup bin/kafka-server-start etc/kafka/server.properties > /dev/null 2>&1 &

/*Creating a topic*/
bin/kafka-topics --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic Idle
bin/kafka-topics --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic Active
bin/kafka-topics --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --topic BigData-console-topic

/*Listening to a topic*/
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic Idle
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic Active

/*Flume Code*/
spark-shell --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1

import org.apache.spark.sql._
import org.apache.spark.sql.types._ 

val userSchema = new StructType()
 .add("Arrival_Time", "string")
 .add("Device", "string")
 .add("gt", "string")

val iot = spark.readStream.format("json")
 .schema(userSchema)
 .option("path", "hdfs:///BigData/flume/simple").load()

val iot_key_christopher = iot.withColumn("key", lit(100))
 .select(col("key").cast("string"), concat(col("Arrival_Time"), lit("  "), col("Device"), lit(" "), col("gt")).alias("value"))
 .filter($"gt" === "sit" || $"gt" === "stand")

val stream_sit_stand = iot_key_christopher.writeStream
 .format("kafka")
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("topic", "Idle")
 .option("checkpointLocation", "file:////home/christo77793/checkpoint")
 .outputMode("append")
 .start()

/*Starting flume stream*/
flume-ng agent --conf /home/christo77793/flume/simple/ -f /home/christo77793/flume/simple/flume-simple.config -Dflume.root.logger=DEBUG, console -n agent
bin/kafka-console-producer --broker-list localhost:9092 --topic  BigData-Console-Topic < records/12-mixed.json


/*Kafka Code*/
import org.apache.spark.sql._
import org.apache.spark.sql.types._ 

val userSchema = new StructType()
 .add("Arrival_Time", "string")
 .add("Device", "string")
 .add("gt", "string")

val iot = spark.readStream.format("json")
 .schema(userSchema)
 .option("path", "hdfs:///BigData/flume/simple").load()

val iot_key_christopher = iot.withColumn("key", lit(100))
 .select(col("key").cast("string"), concat(col("Arrival_Time"), lit("  "), col("Device"), lit(" "), col("gt")).alias("value"))
 .filter($"gt" === "sit" || $"gt" === "stand")

val stream_sit_stand = iot_key_christopher.writeStream
 .format("kafka")
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("topic", "Idle")
 .option("checkpointLocation", "file:////home/christo77793/checkpoint")
 .outputMode("append")
 .start()

def saveToMySql_yadhu = (df: Dataset[Row], batchId: Long) => {
  val url = """jdbc:mysql://localhost:3306/flume_db_yadhu"""

 df
    .withColumn("batchId", lit(batchId))
    .write.format("jdbc")
    .option("url", url)
    .option("dbtable", "final_assignment_table_yadhu")
    .option("user", "root")
    .option("password", "")
    .mode("append")
    .save()
}

 iot
    .writeStream
    .outputMode("append")
    .foreachBatch(saveToMySql_yadhu)
    .start()

val iot_kafka_astha = spark.readStream.format("kafka")
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("subscribe", "BigData-Console-Topic")
 .load()

val iotString_astha = iot_kafka_astha.selectExpr("CAST(value AS STRING)")

val iotDF_astha = iotString_astha.select(from_json(col("value"), userSchema).as("data")).select("data.*")

val iot_key_val_astha = iotDF_astha.withColumn("key", lit(100))
 .select(col("key").cast("string"), concat(col("Arrival_Time"), lit(" "), col("Device"), lit(" "), col("gt")).alias("value"))
 .filter($"gt" === "walk" || $"gt" === "bike" || $"gt" === "stairsup" || $"gt" === "starisdown")

iotDF_astha
    .writeStream
    .outputMode("append")
    .foreachBatch(saveToMySql_yadhu)
    .start()

val stream_yadhu = iot_key_val_astha.writeStream
 .format("kafka")
 .option("kafka.bootstrap.servers", "localhost:9092")
 .option("topic", "Active") .option("checkpointLocation", "file:////home/christo77793/checkpoint1")
 .outputMode("append")
 .start()

/*Starting kafka stream*/
cd confluent-4.1.4
bin/kafka-console-producer --broker-list localhost:9092 --topic  BigData-Console-Topic < records/8-mixed.json

/*Simple code to reset the project (when faced with errors)*/
rm -rf checkpoint
mkdir checkpoint
rm -rf checkpoint1
mkdir checkpoint1
rm -rf -r datasets
mkdir datasets
cp 1-stand.json 2-sit.json 3-stairsdown.json 4-bike.json 5-sit.json 6-stairsup.json 7-walk.json 8-mixed.json 9-mixed.json 10-mixed.json 11-mixed.json 12-mixed.json ~/datasets
hadoop fs -ls /BigData/flume/simple
hadoop fs -rm -r /BigData/flume/simple
hadoop fs -mkdir /BigData/flume/simple