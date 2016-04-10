package org.cg.spark.databroker

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

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * ChannelJobPipeline is spark transformation pipeline which handles spark stream data
 * 
 */
trait ChannelJobPipeline[KEY, EVENT] {

  def handle(
    ssc: StreamingContext,
    messages: InputDStream[(KEY, EVENT)],
    topics: Array[Topic],
    config: Config)
}


/**
 * ChannelProducerTransformer convert the channel topics into spark streams and data producers
 * 
 */
abstract class ChannelProducerTransformer[IN <: Product: TypeTag] extends Transformer[Unit] with Logging {
  def inputStream: DStream[IN]
  def topics: Array[Topic]
  def config: Config

  final val CFG_CLUSTER_NAME = "broker.cluster.name"
  val producerActor = {
    val systemName = config.getString(CFG_CLUSTER_NAME)
    val system = ActorSystem(systemName, config.getConfig("broker").withFallback(ConfigFactory.load)) 
    system.actorOf(Props(new ChannelProducer (systemName, topics(0).channelName)))    
  }

  override def transform() = {
    import scala.collection.JavaConverters._
    val windowStreams = new Array[DStream[IN]](topics.length)

    val systemName = config
    var i = 0
    // driver loop
    topics.foreach { topic =>
      windowStreams(i) = inputStream.window(Seconds(topic.windowSec), Seconds(topic.slideSec))
      // worker loop
      windowStreams(i).foreachRDD { (rdd: RDD[IN], time: Time) =>
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        val df: DataFrame = rdd.toDF()
        df.registerTempTable(topic.name)
        val result = sqlContext.sql(topic.ql)
        val columns = result.columns
        val data = result.collect()
        
        
        try {
          if (data.length>0)
          //listener.onChannelData(topic.name, columns, data)
          producerActor ! Produce(topic.name, columns, data)
        } catch {
          case e: Throwable => logger.error(s"Error in handling sliding window $topic.name", e)
        }
      }
      i = i + 1
    }
  }
}

