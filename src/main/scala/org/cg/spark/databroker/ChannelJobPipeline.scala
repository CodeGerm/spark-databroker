package org.cg.spark.databroker

import scala.reflect.ClassTag
import scala.reflect.internal.Types
import scala.reflect.runtime.JavaUniverse
import scala.reflect.runtime.SymbolTable
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.InputDStream
import org.cg.monadic.transformer.Transformer
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.cg.spark.databroker.ChannelProducer.Produce
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.SQLContextExt

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
  final val CFG_CHKP_INTERVAL = "checkpoint.interval"

  val producerActor = {
    val systemName = config.getString(CFG_CLUSTER_NAME)
    val brokerCfg = config.getConfig("broker")
    logger.info("----- Data Broker Configuration -----")
    logger.info(brokerCfg.toString())
    val system = ActorSystem(systemName, brokerCfg.withFallback(ConfigFactory.load))
    system.actorOf(Props(new ChannelProducer(systemName, topics(0).channelName)).withMailbox("bounded-mailbox"))
  }

  override def transform() = {
    import scala.collection.JavaConverters._
    val windowStreams = new Array[DStream[IN]](topics.length)
    val sqlContext = SQLContext.getOrCreate(inputStream.context.sparkContext)
    //    val conf = inputStream.context.sparkContext.hadoopConfiguration
    //    val interval = Option(conf.getInt(CFG_CHKP_INTERVAL, 600)).getOrElse(600)
    //    inputStream.checkpoint(Seconds(interval))
    val systemName = config

    val schema = ScalaReflection.schemaFor[IN].dataType.asInstanceOf[StructType]

    var i = 0
    // driver loop
    topics.foreach { topic =>
      windowStreams(i) = inputStream.window(Seconds(topic.windowSec), Seconds(topic.slideSec))

      // worker loop
      windowStreams(i).foreachRDD { (rdd: RDD[IN], time: Time) =>

        if (!rdd.isEmpty()) {
          import sqlContext.implicits._

          //val rowRDD = rdd.map { event => Row(event.productIterator.toSeq) }
          //val df = sqlContext.createDataFrame(rowRDD, schema)
          
          val rowRDD = RDDConversions.productToRowRdd(rdd, schema.map(_.dataType))
          val df = SQLContextExt.createDataFrameInternally(sqlContext, rowRDD, schema)
          df.registerTempTable(topic.name)
          val result = sqlContext.sql(topic.ql)
          val columns = result.columns
          val data = result.collect()

          try {
            if (data.length > 0) {
              producerActor ! Produce(topic.name, columns, data)
            }
          } catch {
            case e: Throwable => logger.error(s"Error in handling sliding window $topic.name", e)
          } finally {
            df.unpersist()
            rdd.localCheckpoint()
          }
        }
      }
      i = i + 1
    }
  }
}

