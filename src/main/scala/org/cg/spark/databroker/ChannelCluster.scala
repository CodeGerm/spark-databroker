package org.cg.spark.databroker

import akka.actor.ActorLogging
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.actor.Address
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberEvent
import akka.actor.Actor
import akka.cluster.MemberStatus
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import scala.util.Try
import java.io.File

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**ChannelCluster
 * 
 * The cluster (seed server of the cluster) of all spark channels.
 * 
 * The companion configuration is channel_cluster_server in resource by default
 */
object ChannelCluster {
  
  final val configName = "channel_cluster_server" //name of the configuration file in resource
  final val defaultClusterName = "channelServer" //default name of the cluster
  
  def main(args: Array[String]) {
    val className = this.getClass.getName
    //process cmd params
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: $className <configFile> <clusterName>  
        |  <clusterName> the name of the channel cluster and the name of the Akka system
        |  default cluster name is $defaultClusterName
        """.stripMargin)
    }
    println("Channel Server Starting...")
    val cfgPath = Try(args(0))
    val config = { 
      if (cfgPath.isFailure)
        ConfigFactory.load(configName)
      else
        ConfigFactory.parseFile(new File(cfgPath.get))
    }
    val name = Try(args(1)).getOrElse(defaultClusterName)
    
    val system  = ActorSystem (name, config.withFallback(ConfigFactory.load()))
    val joinAddress = Cluster(system).selfAddress
    Cluster(system).join(joinAddress);
    system.actorOf(Props[ChannelCluster], "ChannelClusterServer")
  }
}

/**
 * ChannelCluster monitors the channel cluster's lifecycle events
 */
class ChannelCluster extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[ChannelCluster])
  }

  override def postStop() = {
    cluster unsubscribe (self)
  }

  var nodes = Set.empty[Address]

  def receive = {
    case state: CurrentClusterState =>
      nodes = state.members.collect {
        case m if m.status == MemberStatus.Up => m.address
      }
    case MemberUp(member) =>
      nodes += member.address
      log.info("Member is Up: {}. {} nodes in cluster",
        member.address, nodes.size)
    case MemberRemoved(member, _) =>
      nodes -= member.address
      log.info("Member is Removed: {}. {} nodes cluster",
        member.address, nodes.size)
    case _: MemberEvent => // ignore
  }
}