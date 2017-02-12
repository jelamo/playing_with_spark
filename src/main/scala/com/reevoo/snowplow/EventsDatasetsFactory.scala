package com.reevoo.snowplow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import com.github.nscala_time.time.Imports._


class EventsDatasetsFactory(val sparkSession: SparkSession) {

  private val hadoopConf = sparkSession.sparkContext.hadoopConfiguration;
  hadoopConf.set("fs.customs3.impl", (new CustomS3FileSystem).getClass.getName());
  hadoopConf.set("fs.customs3.awsAccessKeyId", sys.env("S3_SNOWPLOW_BUCKET_AWS_ACCESS_KEY_ID"))
  hadoopConf.set("fs.customs3.awsSecretAccessKey", sys.env("S3_SNOWPLOW_BUCKET_AWS_SECRET_ACCESS_KEY"))

  val CommonFilterColumns = Array("event_id", "event_type", "domain_sessionid", "network_userid",
    "dvce_created_tstamp", "trkref", "reviewable_context", "additional_properties")

  val BadgeSpecificFilterColumns = Array("hit_type", "content_type", "cta_page_use", "cta_style", "implementation")

  val eventsFilesSelector = new S3BucketFilesSelector

  def getBadgeEventsData(folderName: String) = {
    val badgeEventsDataset = sparkSession.read.format("com.databricks.spark.csv")
      .schema(EventsSchemaDefinitions.BadgeEvent)
      .option("header", "true")
      .option("delimiter","|")
      .option("mode", "DROPMALFORMED")
      .load(eventsFilesSelector.getBadgeEventFiles(folderName):_*)
      .dropDuplicates(Seq("root_id"))

    val rootEventsDataset = getRootEventsData(folderName)

    rootEventsDataset
      .join(badgeEventsDataset, rootEventsDataset("event_id") <=> badgeEventsDataset("root_id"))
      .select((CommonFilterColumns ++ BadgeSpecificFilterColumns).map(new Column(_)):_*)
      .na.fill("").na.fill(0)
  }

  def getConversionEventsData(folderName: String) = {
    val conversionEventsDataset = sparkSession.read.format("com.databricks.spark.csv")
      .schema(EventsSchemaDefinitions.ConversionEvent)
      .option("header", "true")
      .option("delimiter","|")
      .option("mode", "DROPMALFORMED")
      .load(eventsFilesSelector.getConversionEventFiles(folderName):_*)
      .dropDuplicates(Seq("root_id"))

    val rootEventsDataset = getRootEventsData(folderName)

    rootEventsDataset
      .join(conversionEventsDataset, rootEventsDataset("event_id") <=> conversionEventsDataset("root_id"))
      .select(CommonFilterColumns.map(new Column(_)):_*)
      .na.fill("").na.fill(0)
  }

  private def getRootEventsData(folderName: String) = {
    sparkSession.read.format("com.databricks.spark.csv")
      .schema(EventsSchemaDefinitions.RootEvent)
      .option("header", "true")
      .option("delimiter","|")
      .option("mode", "DROPMALFORMED")
      .load(eventsFilesSelector.getRootEventFiles(folderName):_*)
      .filter("app_id='mark'")
      .dropDuplicates(Seq("event_id"))
  }

}
