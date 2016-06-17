package org.cg.spark.databroker

import java.net.URL

import scala.collection.JavaConverters
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.cg.monadic.transformer.TransformationPipelineContext

import com.typesafe.config.Config

import kafka.serializer.StringDecoder
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkStreamingJob

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * Channel broker implementation using spark streaming job (job server job).
 *
 * The assumptions are :
 * 			the message is (String, Event)
 * 			the job configurations is passed through Config
 *
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

class ChannelSparkBroker[EVENT <: Any: ClassTag, DECODER <: StreamingCoder[EVENT]: ClassTag] extends Logging with SparkStreamingJob {

  //configuration nodes
  final val BROKER_CONFIG = "broker"

  //Broker configuration
  final val CFG_BROKER = "bootstrap.servers"
  final val CFG_TOPICS = "topics"
  final val CFG_BATCH_INTERVAL = "batch.interval"
  final val CFG_PIPELINE_CLZ = "pipeline.class"
  final val CFG_CHKP_DIR = "checkpoint.dir"
  final val CFG_CHKP_INTERVAL = "checkpoint.interval"

  val configHelper: TransformationPipelineContext = new TransformationPipelineContext

  def loadConfig(node: String): Config = configHelper.loadConfig(node)

  /**
   * validates the runtime configuration
   */
  override def validate(sc: StreamingContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(
        x => if (TopicUtil.stringToTopicSet(x).length > 0)
          SparkJobValid
        else
          SparkJobInvalid(s"invalidated input.string $x"))
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  /**
   * job server entry
   */
  override def runJob(ssc: StreamingContext, config: Config): Any = {

    log.info("============JOB CONFIG BEGIN==============")
    log.info(config.toString)
    log.info("============JOB CONFIG END==============")
    log.info("============JOB PARAMS BEGIN==============")
    val topics = TopicUtil.stringToTopicSet(config.getString("input.string"))
    topics.foreach { x => log.info(x.toString) }
    log.info("============JOB PARAMS END==============")
    val streamCfg = initConfig(config)
    log.info("============STREAM CONFIG BEGIN==============")
    log.info(streamCfg.toString)
    log.info("============STREAM CONFIG END==============")

    //val ssc = new StreamingContext(sc, Seconds(6))     
    internalRun(topics, ssc, streamCfg, config)
    log.info("Engine starting")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   *  init configuration by using parameters
   */
  def initConfig(config: Config): Config = {
    configHelper.initIfUndefined(config)
    loadConfig(BROKER_CONFIG)
  }

  /**
   *  init configuration by using parameters
   */
  def initConfig(args: Array[String]): Config = {
    val configURLString = args(0)
    //load config
    try { URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory()) } catch { case e: Throwable => ; };
    val configURL = new URL(configURLString)
    configHelper.initIfUndefined(configURL)
    loadConfig(BROKER_CONFIG)
  }
  /**
   *  internalRun
   */
  def internalRun(topics: Array[Topic], ssc: StreamingContext, streamCfg: Config, config: Config): Any = {

    // get params
    import scala.collection.JavaConverters._
    val brokers = streamCfg.getString(CFG_BROKER)
    val topicsSet = streamCfg.getStringList(CFG_TOPICS).asScala.toSet

    val chkpointDir = if (Try(streamCfg.getString( CFG_CHKP_DIR)).isFailure) None else Some(streamCfg.getString(CFG_CHKP_DIR) + "/" + this.getClass.getName)
    val chkpointInterval = Try(streamCfg.getLong(CFG_CHKP_INTERVAL)).getOrElse(300L)

    //load pipeline class and init instance from configuration
    val pipelineClzName = streamCfg.getString(CFG_PIPELINE_CLZ)
    val clz = Class.forName(pipelineClzName)
    val pipeLine = clz.newInstance()

    if (chkpointDir.isDefined)
      ssc.checkpoint(chkpointDir.get)
    
    // populate pass in config to spark conf  
    val conf = ssc.sparkContext.hadoopConfiguration  
    import scala.collection.JavaConverters._
    streamCfg.entrySet().asScala.foreach(e => conf.set(e.getKey, e.getValue.unwrapped().toString()));
    
    log.info(s"load pipeline $clz")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, EVENT, StringDecoder, DECODER](
      ssc, kafkaParams, topicsSet)
    // checkpoint is to resolve linage issue instead of recovery  
    if (chkpointDir.isDefined)
      messages.checkpoint(Seconds(chkpointInterval))

    pipeLine match {
      case p: ChannelJobPipeline[String, EVENT] => { log.info("begin handling"); p.handle(ssc, messages, topics, config) }
      case _                                    => { log.error(s"wrong event type $pipeLine"); throw new ClassCastException }
    }
  }

  /**
   * main
   */
  def main(args: Array[String]) {
    val className = this.getClass.getName
    //process cmd params
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: $className <configurationUrl> <topics> 
        |  <configurationUrl> the URL path of the config file 
        |  <topics> topic1|select * from topic1|5|5||topic2|select * from topic2|10|10
        |  
        """.stripMargin)
      System.exit(1)
    }

    // load parameter from config file running standalone
    val streamCfg = initConfig(args)
    // get params
    import scala.collection.JavaConverters._
    val batchInterval = Option(streamCfg.getLong(CFG_BATCH_INTERVAL)).getOrElse(5L)

    val conf = new SparkConf().setAppName("ChannelEngine")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(batchInterval))
    val topics = TopicUtil.stringToTopicSet(args(1))
    internalRun(topics, ssc, streamCfg, configHelper.config.get)

    ssc.start()
    ssc.awaitTermination()
  }

}