# Spark-ETL-Project

In this Final Project we mimic an ETL process using Spark Streaming as well as different data sources and sinks to get see how such systems are developed & implemented.

The ETL process has two sources:
* A Flume agent writing data into an HDFS source.
* A Kafka producer writing data into a topic on a Kafka server

For this assignment, we used IOT data from a Nexus phone. Spark streaming will read the data from the above mentioned two sources and then the data will then be aggregated and sent to two different sinks depending on the type of data that it is. Any data that involves sitting/standing, the data is sent to a Kafka topic called “idle”. and any other data is sent to a Kafka topic called “active”.
The 2 consumers read the data and display the activity and time according to the activity type and finally, all data is sent and saved to a SQL database specifically in this case MySQL.
