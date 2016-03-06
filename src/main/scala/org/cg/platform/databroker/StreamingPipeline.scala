package org.cg.platform.databroker

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import com.typesafe.config.Config


/**
 * 
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

trait StreamingPipeline [KEY,EVENT] {
  
   def handle(ssc: StreamingContext, messages: InputDStream[(KEY, EVENT)], config: Config)

}