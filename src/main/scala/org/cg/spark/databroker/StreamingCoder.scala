package org.cg.spark.databroker

import kafka.serializer.Decoder
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import scala.reflect.ClassTag

/**
 * 
 * @author Yanlin Wang (wangyanlin@gmail.com)
 *
 */

trait StreamingCoder[Event] extends Decoder[Event] with Encoder[Event]  {
  
}