package org.cg.spark.databroker.example

import org.cg.spark.data.impl.SparkJobServerChannel
import org.cg.spark.data.IChannelListener
import org.cg.spark.data.SourceFeed
import org.cg.spark.data.Request
import org.cg.spark.data.impl.BasicChannel



/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

object BasicChannelExample extends IChannelListener {

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

object JobServerChannelExample extends IChannelListener {

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
    //val channel = SparkJobServerChannel("http://192.168.99.100:18090", "/Users/yanlinwang/Documents/centrify/git/platform/com.centrify.platform.databroker/target/com.centrify.platform.databroker-0.0.1-SNAPSHOT.jar", channelName, 6000)
    val channel = SparkJobServerChannel("http://192.168.99.100:18090", channelName, 6000)
    
    channel.registerListener(this)
    channel.init()
    println(channel.actorUrl)
    val ret1 = channel.requestFeed(new Request("com.centrify.platform.databroker.impl.EventFeedEngine", Array("file:///opt/mount/host/Documents/centrify/git/platform/com.centrify.platform.databroker/conf/datachannel.conf", channelName, s"""\"select * from $channelName\"""","6","6")))
    println(ret1)
    
  }

}