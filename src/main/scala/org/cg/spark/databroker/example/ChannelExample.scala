package org.cg.spark.databroker.example

import org.cg.spark.data.impl.SparkJobChannel
import org.cg.spark.data.impl.BasicChannel
import org.cg.spark.data.IChannelListener
import org.cg.spark.data.SourceFeed
import org.cg.spark.data.Request

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

object ChannelExample extends IChannelListener {

  def onFeed(feed: SourceFeed) {
    feed.data.foreach { x => println(x) }
  }

  def main(args: Array[String]) {
    val className = this.getClass.getName
    //process cmd params
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: $className <channelName> 
        |  
        """.stripMargin)
      System.exit(1)
    }

    val Array(channelName) = args
    val channel = BasicChannel(channelName, 5)

    channel.registerListener(this)
    channel.init()
    println(channel.actorUrl)
    channel.requestFeed(new Request(null, null));
  }

}