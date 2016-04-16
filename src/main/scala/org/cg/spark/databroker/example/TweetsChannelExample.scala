package org.cg.spark.databroker.example

import org.apache.spark.sql.Row
import org.cg.spark.databroker.ChannelJob
import org.cg.spark.databroker.ChannelJobManager
import org.cg.spark.databroker.IChannelListener
import org.cg.spark.databroker.JobServerQuorum
import org.cg.spark.databroker.Topic

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * Example to demonstrate Channel API (create and run the channel and subscribe/unsubscribe the channel).
 * 
 * In order to run the example successfully, here are the general steps:
 * 0. make sure update tweets-kafka.conf with proper twitter credential.
 * 		run kafka and create topic tweets
 *   
 * 1. build the project using maven, mvn install
 * 
 * 2. lunch docker-compose in docker folder. This will lunch channel cluster server, spark, yarn and hdfs together
 * 
 * 3. run TweetsChannelExample. This will create and run an example channel with 2 streaming sql query against incoming tweets stream
 * 
 * 4. run TweetsKafkaProducer. This will connect to your twitter account and produce tweets into kafka tweets topic.
 *  
 */
object TweetsChannelExample extends IChannelListener{
  
  def onChannelData(topic: String, columns: Array[String], data: Array[Row]){
    println (s"receive data from $topic")
    data.foreach { row => println (row) }
  }
  
  def main (args : Array[String] ) {
    val channelName = "tweets_channel"
     val topics = Array [Topic] (
          Topic ("all_records", "select * from all_records", 5, 5, channelName),
          Topic ("top_users", "select name, count(*) as tweets from top_users group by name order by tweets", 60, 60, channelName)
         )
     val job =  new ChannelJob(channelName, "org.cg.spark.databroker.example.TweetsSparkBroker", topics, 5) 
     
     val manager  =  new ChannelJobManager (new JobServerQuorum(Array ("http://192.168.99.100:18090")))
  
     println ("running job : " + manager.runChannelJob(job, None))    
    
     println ("subscribe all_records ? (press any key)")
     System.in.read()        
     manager.subscribeTopic(channelName, "all_records", this)
     
     println ("subscribe top_users ? (press any key)")
     System.in.read()        
     manager.subscribeTopic(channelName, "top_users", this)
     
     println ("ubsubscribe all_records ? (press any key)")
     System.in.read()     
     manager.unSubscribeTopic(channelName, "all_records")
     
     println ("ubsubscribe top_users ? (press any key)")
     System.in.read()     
     manager.unSubscribeTopic(channelName, "top_users")
     
     println ("stop job ? (press any key)")
     System.in.read()     
     manager.stopChannelContext(job.name)
  }
}