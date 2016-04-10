package org.cg.spark.databroker

import org.apache.spark.sql.Row

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */


/**
 *  The actual data listener to get pushed data from channel
 */
trait IChannelListener {
  def onChannelData(topic: String, columns: Array[String], data: Array[Row])
}