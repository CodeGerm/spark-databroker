package org.cg.spark.databroker

import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorLogging
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberEvent
import org.apache.spark.sql.Row
import akka.actor.Props
import akka.contrib.pattern.DistributedPubSubMediator
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import org.apache.spark.Logging

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * ChannelProducer companion object
 */
object ChannelProducer extends Logging{
  @transient def props(name: String): Props = Props(classOf[ChannelProducer], name)
  case class Produce(topic: String, columns: Array[String], data: Array[Row])
  case class Message (from : String, topic: String, columns: Array[String], data: Array[Row])
}

/**
 * ChannelProducer is actor which produce the channel data 
 */
class ChannelProducer(name: String, channel : String) extends Actor with ActorLogging {
  @transient val cluster = Cluster(context.system)
  @transient val mediator = DistributedPubSubExtension(context.system).mediator
  //mediator ! Subscribe(channel, self)
  log.info(s"join channel cluster $channel")

  override def preStart(): Unit =
    cluster.subscribe(self, classOf[MemberEvent])

  override def postStop(): Unit =
    cluster unsubscribe self

  def receive = {
    case ChannelProducer.Produce(topic, columns, data) => {
      log.info(s"receive data feed from channel/topic $topic")
      mediator ! Publish (ChannelUtil.clusterTopic(channel, topic), ChannelProducer.Message (this.name, topic, columns, data))
    }
    case ChannelProducer.Message(from, topic, columes, data) =>
      println (s"from $from, topic $topic, $columes(0), $data(0)) ")
  }
}