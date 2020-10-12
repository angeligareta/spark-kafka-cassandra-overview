package KafkaSpark

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.streaming._

import scala.collection.immutable.HashMap

object KafkaSpark {

  def main(args: Array[String]) {
    if (args.length != 3) {
      throw new Exception("Required arguments must be received to start the program: (<topic_name>)")
    }

    /** INPUT CONFIGURATION */
    // Receive kafka key space and topic name by program arguments
    val keyspaceName = args(0)
    val tableName = args(1)
    val topicName = args(2)

    /** CASSANDRA INITIALIZATION */
    // Initialize Cassandra cluster and session. Usually it is better to specify more than one contact point just in case that single contact point is unavailable.
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // Build a key space using simple strategy with only one replica and create table
    // where this program will store the result of the streaming computation
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${keyspaceName} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute(s"CREATE TABLE IF NOT EXISTS ${keyspaceName}.${tableName} (word text PRIMARY KEY, count float);")

    /** SPARK STREAMING INITIALIZATION */
    // Initialize StreamingContext object => Main entry point for Spark Streaming functionality.
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSpark")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // Set checkpoint directory (necessary to work)
    ssc.checkpoint("tmp")

    // Initialize [Kafka Consumer Configuration](http://kafka.apache.org/documentation.html#consumerconfigs)
    val kafkaConsumerParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092" // List of host/port pairs to use for establishing the initial connection to the Kafka cluster
      //      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer", // Deserializer class for key.
      //      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer", // Deserializer class for value.
      //      ConsumerConfig.GROUP_ID_CONFIG -> "KafkaSpark", // A unique string that identifies the consumer group this consumer belongs to.
      //      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false" // If true the consumer's offset will be periodically committed in the background.
    )

    /** SPARK STREAMING EXECUTION */
    // Subscribe to topic using second approach => [Direct Stream](https://medium.com/@rinu.gour123/apache-kafka-spark-streaming-integration-af7bd87887fb
    val topicStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConsumerParams, Set(topicName))

    // Transform the received stream into pairs <key, value> (only if the format received is correct, eg: 'z,3')
    val topicStreamPairs: DStream[(String, Double)] = topicStream
      .map(record => {
        val rawValues = record._2.split(",")
        // If format is not correct, return null
        if (rawValues.length != 2) {
          null
        } else {
          (rawValues(0), rawValues(1).toDouble)
        }
      })
      .filter(pair => pair != null)

    // Mapping function that continuously calculates the average between values of the same key and store them in state
    val avgMappingFunction = (key: String, value: Option[Double], state: State[HashMap[String, Double]]) => {
      // If value is not empty (timeout) | Otherwise state should not be updated
      if (value.isDefined) {
        // If state is new, initialize with empty hash map
        if (!state.exists()) {
          state.update(new HashMap[String, Double]())
        }
        // If key was not present in state, average would equal value
        val average = (value.get + state.get.getOrElse(key, value.get)) / 2
        // Update local state
        state.update(state.get + (key -> average))
        // Return current average for that key
        (key, average)
      }
      else {
        null
      }
    }
    // Map with state across RDD
    val topicStreamPairsAvg = topicStreamPairs.mapWithState(StateSpec.function(avgMappingFunction)).filter(pair => pair != null)

    // Save result to cassandra
    topicStreamPairsAvg.saveToCassandra(keyspaceName, tableName, SomeColumns("word", "count"))

    // Start streaming job
    ssc.start()

    // Wait until error occurs or user stops the program and close Cassandra session
    ssc.awaitTermination()
    session.close()
  }
}
