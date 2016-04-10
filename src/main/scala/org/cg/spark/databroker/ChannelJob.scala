package org.cg.spark.databroker


/**
 * ChannelJob.id => job server job id, None in case of job creation
 * ChannelJob.name => Job Context; primary key of a job
 * ChannelJob => Streaming Job
 * ChannelJob.app => Job App Name
 * serverId =>  use the url as key to identify the server channel is on when retrieve jobs from server. Set to None during creation
 * 
 * The app name/class name should be the class extends ChannelSparkBroker spark job
 */
case class ChannelJob(id : Option[String], name: String, className: String, topics: Array[Topic], serverId : Option[String], isRunning: Boolean , intervalSec: Int ) {
     def this( name: String, className: String, topics: Array[Topic], intervalSec: Int ) = this(None, name, className, topics, None, false , intervalSec);
}



/**
 * Channel Topic
 * 
 * name : name of the topic
 * ql   : spark ql for a predefined moving window 
 * windowSec : moving window size in seconds
 * slideSec  : sliding inverval in seconds
 * channelName : the name of the channel that topic belongs to
 * 
 * The unique identified of the topic in a cluster is channelName_topicName
 */
case class Topic(name: String, ql: String, windowSec: Int, slideSec: Int, channelName : String)


/**
 * Channel related utilities
 */
object ChannelUtil {
  
  /**
   * produce the key of cluster topic  
   */
  def clusterTopic (channel: String , topic: String)={
    channel + "_" + topic
  }
}