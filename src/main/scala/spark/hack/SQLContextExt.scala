package org.apache.spark.sql
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object SQLContextExt {
  def createDataFrameInternally(sqlContext: SQLContext, catalystRows: RDD[InternalRow],
                                schema: StructType): DataFrame = {
    sqlContext.internalCreateDataFrame(catalystRows, schema)
  }

}