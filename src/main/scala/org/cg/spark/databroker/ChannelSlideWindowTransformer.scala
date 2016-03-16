package org.cg.spark.databroker

import org.cg.monadic.transformer.spark.DStreamSlideWindowTransformer
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.Row
import org.cg.spark.data.impl.SparkJobChannelSource
import org.cg.spark.data.ChannelState
import org.apache.spark.Logging

/**
 * Spark moving window transformer, channel data feed enabled
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */
abstract class ChannelSlideWindowTransformer[IN <: Product: TypeTag] extends DStreamSlideWindowTransformer[IN] with Logging {
  def channelUrl: String
  
  //one source per channel
  @transient object Source extends SparkJobChannelSource(name, channelUrl) {
    init()
  }

  override def onData (name: String, columns: Array[String], data: Array[Row]): Unit = {
    Source.doReady(columns)
    if (data.length < 1) {
      return ;
    }
    Source.doFeed(data)
  }
}