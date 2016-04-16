package org.cg.spark.databroker.example

import org.cg.spark.databroker.ChannelSparkBroker
import org.cg.spark.databroker.ChannelJobPipeline
import kafka.serializer.StringDecoder
import kafka.serializer.StringDecoder
import org.cg.spark.databroker.StreamingCoder
import org.cg.spark.databroker.Topic
import com.twitter.bijection.avro.SpecificAvroCodecs
import kafka.serializer.DefaultDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import com.typesafe.config.Config
import org.cg.monadic.transformer.Transformer
import org.apache.spark.Logging
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import org.cg.spark.databroker.ChannelProducer.Produce
import org.cg.spark.databroker.ChannelProducerTransformer
import org.cg.spark.databroker.StreamingCoder
import kafka.serializer.DefaultEncoder
import kafka.utils.VerifiableProperties

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * TweetsSparkBroker
 *
 * The main entry class of Channel implementation. It runs within spark
 *
 */
object TweetsSparkBroker extends ChannelSparkBroker[Array[Byte], DefaultCoder] {

}

/**
 * DefaultCoder
 *
 * Serialization and de-serialization
 *
 */
class DefaultCoder(prop: VerifiableProperties) extends StreamingCoder[Array[Byte]] {
  val decoder = new DefaultDecoder
  val coder = new DefaultEncoder

  def fromBytes(bytes: Array[Byte]): Array[Byte] = decoder.fromBytes(bytes)

  def toBytes(event: Array[Byte]): Array[Byte] = coder.toBytes(event)
}

/**
 * TweetRecord
 *
 * case class for data frame to query
 */
case class TweetRecord(name: String, text: String, count: Int)

/**
 * TweetsSparkPipeline
 *
 * The pipeline class that is loaded by broker (TweetsSparkBroker). The spark broker class is 
 * looking for broker.pipeline.class in the configuration then initiate the instance of 
 * ChannelJobPipeline. The pipeline is composed by one of many transformers.
 *
 */
class TweetsSparkPipeline extends ChannelJobPipeline[String, Array[Byte]] {

  def handle(ssc: StreamingContext, messages: InputDStream[(String, Array[Byte])], topics: Array[Topic], config: Config) = {

    val pipe = for {
      tweets <- TweetsTransformer(messages)
      window <- TweetsWindowTransformer(tweets, topics, config)
    } yield (window)
    pipe.transform()
  }

}

/**
 * TweetsTransformer
 * 
 * transform DStream from kafka to TweetRecord
 */
case class TweetsTransformer(inputDstream: DStream[(String, Array[Byte])]) extends Transformer[DStream[TweetRecord]] {
  override def transform() = {
    inputDstream.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption).map { tweet => TweetRecord(tweet.getName.toString(), tweet.getText.toString(), 0) }
  }
}

/**
 * TweetsWindowTransformer
 * 
 * extends ChannelProducerTransformer to deal with TweetRecord 
 */
case class TweetsWindowTransformer(inputStream: DStream[TweetRecord],
                                   topics: Array[Topic],
                                   config: Config)
    extends ChannelProducerTransformer[TweetRecord] {
}
