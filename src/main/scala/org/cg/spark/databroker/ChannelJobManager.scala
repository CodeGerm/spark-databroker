package org.cg.spark.databroker

import org.apache.spark.Logging
import org.khaleesi.carfield.tools.sparkjobserver.api.ISparkJobServerClientConstants
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobConfig
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobBaseInfo
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import scala.collection.mutable.Map
import akka.actor.ActorRef
import scala.collection.mutable.HashMap
import scala.collection.mutable.ConcurrentMap
import scala.collection.convert.Wrappers.ConcurrentMapWrapper
import scala.collection.concurrent.Map
import java.util.concurrent.ConcurrentHashMap
import akka.cluster.Cluster
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Publish
/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * ChannelJobManager manages the lifecycle of the channel and its spark job
 *
 *
 * ChannelJob.name => Job Context; primary key of a job and qunique within a quorom
 * ChannelJob => Streaming Job
 * ChannelJob.app => Job App Name, in the managed context, app name is the same as class name
 *
 * serverUrls : job server urls
 * jobJarPath : optional parameter, the path of the channel job jar file
 */

class ChannelJobManager(quorom: JobServerQuorum) extends Logging {

  val RUNNING = "RUNNING"
  val STARTED = "STARTED"

  //job server
  val PARAM_CONTEXT_FACTORY = "context-factory"
  val PARAM_STREMAING_INTERVAL = "streaming.batch_interval"
  val PARAM_STREMAING_STOPGRACEFULLY = "streaming.stopGracefully"
  val PARAM_STREMAING_STOPCONTEXT = "streaming.stopSparkContext"

  /**
   * list all channel jobs on all job servers in the quorum
   *
   * return map of [channelName, ChannelJob]
   * channelName is job server context name
   *
   * Note: the job stream interval is not accurate, we need to get it from context
   */
  def listChannelJobs: scala.collection.immutable.Map[String, ChannelJob] = {
    import scala.collection.JavaConverters._
    quorom.jobClientsMap.flatMap { p =>
      p match {
        case (cid, client) =>
          val jobs = client.getJobs.asScala
          jobs.map { job =>
            val ctx = job.getContext
            val id = job.getJobId
            val cfg = client.getConfig(id)
            val channelJob = new ChannelJob(Some(id), ctx, job.getClassPath, TopicUtil.cfgToTopic(cfg), Some(cid), isRunning(job.getStatus), 10)
            ctx -> channelJob
          }
      }
    }
  }

  /**
   * find the channel job by channel name
   */
  def findChannelJob(channelName: String): Option[ChannelJob] = {
    Option(listChannelJobs(channelName))
  }

  def listRunningChannelJobs(): scala.collection.immutable.Map[String, ChannelJob] = {
    listChannelJobs.filter(p => p match {
      case (id, job) => job.isRunning
    })
  }

  /**
   * find if the channel (job server streaming context) is running
   *
   * returns the id (url) of the server that's running the channel (context)
   */
  def findRunningChannel(channelName: String) = {
    listRunningChannelJobs().filter(p => p match { case (key, job) => job.isRunning })
  }

  /**
   * runChannelJob => get or create context and run the job (create & start context)
   *
   */
  def runChannelJob(job: ChannelJob, jarPath: Option[String] = None): Boolean = {
    import scala.collection.JavaConverters._
    //check if channel job's context is running
    if (!findRunningChannel(job.name).isEmpty) {
      log.error(s"failed to run job $job, it is running already")
      true
    } else {
      //create streaming context
      val params = Map(PARAM_CONTEXT_FACTORY -> "spark.jobserver.context.StreamingContextFactory",
        PARAM_STREMAING_INTERVAL -> String.valueOf(job.intervalSec * 1000),
        PARAM_STREMAING_STOPCONTEXT -> String.valueOf(true),
        PARAM_STREMAING_STOPGRACEFULLY -> String.valueOf(true))
      val opt = quorom.jobServerClient
      if (!opt.isDefined) {
        val msg = s"failed to run job $job, quorom is not avaliable"
        log.error(msg)
        throw new IllegalStateException(msg)
      }
      val client = opt.get
      if (!client.getContexts().contains(job.name)) {
        log.info(s"creating context for job $job")
        if (!client.createContext(job.name, params.asJava)) {
          val msg = s"failed to create context for job $job"
          log.error(msg)
          throw new IllegalStateException(msg)
        }
      } else {
        log.info(s"context exists for job $job")
      }

      log.info(s"run channel job $job")
      val jobParams = Map(
        ISparkJobServerClientConstants.PARAM_APP_NAME -> job.className,
        ISparkJobServerClientConstants.PARAM_CONTEXT -> job.name,
        ISparkJobServerClientConstants.PARAM_CLASS_PATH -> job.className);
      val result = client.startJob("input.string = " + TopicUtil.topicsToString(job.topics), jobParams.asJava);
      log.info(s"run channel job $job with status: $result")
      isRunning(result.getStatus)
    }
  }

  /**
   * stopChannelJob => stop & remove the running context
   */
  def stopChannelJob(channelName: String) {
    findRunningChannel(channelName).foreach(p => p match { case (key, job) => quorom.jobClientsMap.get(job.serverId.get).get.deleteContext(channelName) })
  }

  // subscriber manager
  final val CFG_CLUSTER_NAME = "broker.cluster.name"
  val system = {
    val config = ConfigFactory.load("channel_subscriber")
    val systemName = config.getString(CFG_CLUSTER_NAME)
    ActorSystem(systemName, config)
  }

  val cluster = Cluster(system)
  val mediator = DistributedPubSubExtension(system).mediator

  import scala.collection.convert.decorateAsScala._
  val registry = new ConcurrentHashMap[String, ActorRef]().asScala

  /**
   * subscribeTopic => subscribe
   */
  def subscribeTopic(channel: String, topic: String, listener: IChannelListener) {
    registry.getOrElse(ChannelUtil.clusterTopic(channel, topic), system.actorOf(Props(new ChannelSubcriber(channel, topic, listener))))
  }

  def unSubscribeTopic(channel: String, topic: String) {
    mediator ! Publish(ChannelUtil.clusterTopic(channel, topic), ChannelSubcriber.Stop)
    registry.remove(ChannelUtil.clusterTopic(channel, topic))

  }

  private def isRunning(status: String) = {
    "RUNNING".equals(status) || "STARTED".equals(status)
  }

}

/**
 * The job arguments is passed as data: String into job server client. In the channel the data = list of topics
 */
object TopicUtil {

  final val TOPIC_SEP_REG = """\|\|"""
  final val ARG_SEP_REG = """\|"""

  final val TOPIC_SEP = "||"
  final val ARG_SEP = "|"

  def stringToTopicSet(input: String): Array[Topic] = {
    val topics = input.split(TOPIC_SEP_REG).toArray
    topics.map { topic =>
      val args = topic.split(ARG_SEP_REG).toArray
      val Array(topicName, sql, window, interval, channelName) = args
      Topic(topicName, sql, window.toInt, interval.toInt, channelName)
    }
  }

  def topicsToString(topics: Array[Topic]): String = {
    val builder = new StringBuilder
    topics.foldLeft("")((a, b) => if (a.length() > 0) a + TOPIC_SEP + topicToString(b) else topicToString(b))
  }

  def topicToString(topic: Topic): String = {
    val builder = new StringBuilder
    builder.append(topic.name)
    builder.append(ARG_SEP)
    builder.append("\"")
    builder.append(topic.ql)
    builder.append("\"")
    builder.append(ARG_SEP)
    builder.append(topic.windowSec)
    builder.append(ARG_SEP)
    builder.append(topic.slideSec)
    builder.append(ARG_SEP)
    builder.append(topic.channelName)
    builder.toString()
  }

  def cfgToTopic(cfg: SparkJobConfig): Array[Topic] = {
    val input = cfg.getConfigs.get("input")
    val args = input match {
      case obj: net.sf.json.JSONObject => {
        Option(obj.get("string").toString)
      }
      case _ => None
    }
    if (args.isEmpty)
      new Array[Topic](0)
    else
      stringToTopicSet(args.get)
  }

}