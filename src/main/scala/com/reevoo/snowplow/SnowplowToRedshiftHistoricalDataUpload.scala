package com.reevoo.snowplow

import org.apache.spark.sql.SparkSession
import com.github.nscala_time.time.Imports._
import org.joda.time.Days
import com.frugalmechanic.optparse._

object SnowplowToRedshiftHistoricalDataUpload {

  object CommandLineArguments extends OptParse {
    val initial = BoolOpt()
    val fromDate = StrOpt()
    val toDate = StrOpt()
    val destinationTable = StrOpt()
    val overwrite = BoolOpt()
  }

  final lazy val sparkSession = SparkSession.builder()
    .appName("Spark Snowplow S3 Uploader")
    .config("spark.some.config.option", "some-value").getOrCreate()

  final lazy val EventsDatasetsFactory = new EventsDatasetsFactory(sparkSession)

  final lazy val EventsDatasetsUploader = new EventsDatasetsUploader(sparkSession, destinationTable)

  final lazy val destinationTable = CommandLineArguments.destinationTable.getOrElse(
    RedshiftJDBConnector.DefaultDestinationTable)

  def main(args: Array[String]) {

    CommandLineArguments.parse(args)

    RedshiftJDBConnector.createOrTruncateMarkEventsTable(destinationTable, CommandLineArguments.overwrite)

    if (CommandLineArguments.fromDate && CommandLineArguments.toDate) {
      val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      val fromDate = new DateTime(CommandLineArguments.fromDate.get)
      val toDate = new DateTime(CommandLineArguments.toDate.get)

      (0 to Days.daysBetween(fromDate, toDate).getDays()).map(fromDate.plusDays(_)).foreach(date => {
        println(s"*** PROCESSING FILES FOR DATE ${date} FOLDERS")
        processEvents(DateFormatter.print(date))
      })
    }

    if (CommandLineArguments.initial) {
      println(s"*** PROCESSING FILES FOR _INITIAL FOLDER")
      processEvents("_initial")
    }

    sparkSession.stop()

  }

  private def processEvents(folderName: String) = {
    EventsDatasetsUploader.upload(EventsDatasetsFactory.getBadgeEventsData(folderName))
    EventsDatasetsUploader.upload(EventsDatasetsFactory.getConversionEventsData(folderName))
  }

}
