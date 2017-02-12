package com.reevoo.snowplow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

class EventsDatasetsUploader(val sparkSession: SparkSession, val destinationTable: String) {

  def upload(dataset: DataFrame) = {
    dataset.write.mode(SaveMode.Append)
      .jdbc(RedshiftJDBConnector.DbUrl,
        destinationTable,
        RedshiftJDBConnector.connectionProperties)
  }

}
