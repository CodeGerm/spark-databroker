package org.cg.spark.databroker.example

import java.util.Properties
import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import twitter4j.{Status, FilterQuery}
import twitter4j.TwitterStream
import org.cg.spark.databroker.example.TwitterStream.OnTweetPosted

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * TweetsKafkaProducer
 * 
 * produce the tweets into avro message and send to kafka
 * 
 */
object TweetsKafkaProducer {
    private val conf = ConfigFactory.load("tweets-kafka")

  val KafkaTopic = "tweets"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  val filterUsOnly = new FilterQuery().locations(Array(
    Array(-126.562500,30.448674),
    Array(-61.171875,44.087585)))


  def main (args: Array[String]) {
    val twitterStream = TwitterStream.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))
    twitterStream.filter(filterUsOnly)
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafka(t:Tweet) {
    println(t.toString())
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)
  }


}