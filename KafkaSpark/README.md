# Kafka, Spark and Cassandra Integration
The purpose of this project is to develop a Spark Streaming application that reads Apache Kafka's data stream, calculates some result based on the data, and continually stores the result in Cassandra's data warehouse. The input data is <key, value> pairs in the form of <String, Int>, and the goal is to calculate the average value of each key and continuously update it, while new pairs arrive.

## Initialization
### Kafka
As Kafka uses ZooKeeper to mantain the configuration information, we need to start a ZooKeeper server followed by the Kafka server.
```
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Next, we need to create a topic, which is the feed where the messages will be published.
```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <topic_name>
```
### Cassandra
After that, Cassandra server must be started. If you want to see the logs in the foreground, you can use the option "-f".
```
$CASSANDRA_HOME/bin/cassandra -f
```
## Programs
### Producer
The first program we are going to start is the generator. This will continously generate pairs of <String,Int> and feed them to the specified Kafka topic. It basically generates random letters from the english alphabet followed by a value in the range of [0, 26].

#### Execution
To run it, you need to execute the following command:
```

```
### Consumer

#### Execution
To run it, you need to execute the following command:
```

```
