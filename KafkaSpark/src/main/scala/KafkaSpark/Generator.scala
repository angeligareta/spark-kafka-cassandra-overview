package KafkaSpark

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object Generator {
  val alphabet: Seq[Char] = 'a' to 'z'
  val randomGenerator = new Random()

  def generateRandomMessage: String = {
    val alphabetIndex = randomGenerator.nextInt(alphabet.size)
    val key = alphabet(alphabetIndex)
    val value = randomGenerator.nextInt(alphabet.size)
    s"${key},${value}"
  }

  def main(args: Array[String]) {
    val topicName = args(0)

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "Generator")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      val data = new ProducerRecord[String, String](topicName, null, generateRandomMessage)
      producer.send(data)
      print(data + "\n")

      Thread.sleep(50) // wait for 0.05 s
    }

    producer.close()
  }
}