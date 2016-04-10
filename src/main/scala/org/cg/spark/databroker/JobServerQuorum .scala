package org.cg.spark.databroker

import org.khaleesi.carfield.tools.sparkjobserver.api.ISparkJobServerClient
import scala.collection.mutable.ArrayBuffer
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientFactory
import scala.util.Try

/**
 *
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

/**
 * Quorum => list of possible job servers. jobClient method will get a job server client using round robin fashion
 *
 * serverUrls : job server urls
 * jobJarPath : optional parameter, the path of the channel job jar file
 */
class JobServerQuorum(serverUrls: IndexedSeq[String]) {

  private var idx = 0

  private val jobClients: IndexedSeq[ISparkJobServerClient] = {
    serverUrls.map(url => SparkJobServerClientFactory.getInstance().createSparkJobServerClient(url))
  }

  val jobClientsMap: Map[String, ISparkJobServerClient] = {
    serverUrls.map(url => url -> SparkJobServerClientFactory.getInstance().createSparkJobServerClient(url)).toMap
  }

  private def getAvaliableInternal(startIndex: Int, round: Int = 0): (Int, Option[ISparkJobServerClient]) = {

    if (round == jobClients.length) {
      (startIndex, None)
    } else {
      val client = jobClients(startIndex)

      if (Try(client.getJobs).isSuccess) {
        (startIndex, Some(client))
      } else {
        val nextIndex = (startIndex + 1) % jobClients.length
        getAvaliableInternal(nextIndex, round + 1)
      }
    }
  }

  /*
   * find job server client in a round robin manner
   */
  def jobServerClient: Option[ISparkJobServerClient] = {
    if (jobClients.isEmpty) {
      None
    } else {
      val startIndex = (idx + 1) % jobClients.length
      val (newIndex, result) = getAvaliableInternal(startIndex)
      idx = newIndex
      result
    }
  }

  def jobServerClients = jobClients
}