package org.cg.spark.databroker

import akka.cluster.Cluster
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.contrib.pattern.DistributedPubSubExtension
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * channel subscriber actor, receive data from cluster and pipe it to IChannelListener interface
 */
object ChannelSubcriber {
  case class Stop ()
}

class ChannelSubcriber(channel: String, topic: String, listener: IChannelListener) extends Actor with ActorLogging {
  @transient val cluster = Cluster(context.system)
  @transient val mediator = DistributedPubSubExtension(context.system).mediator

  mediator ! Subscribe(ChannelUtil.clusterTopic(channel, topic), self)
  log.info(s"join topic $topic")

  override def postStop(): Unit =
    cluster unsubscribe self

  def receive = {
        case ChannelProducer.Message(from, topic, columns, data) =>
          listener.onChannelData(topic, columns, data)
        case ChannelSubcriber.Stop =>
          context stop self          
  }
}