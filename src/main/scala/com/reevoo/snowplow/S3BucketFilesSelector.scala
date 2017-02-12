package com.reevoo.snowplow

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.ObjectListing
import scala.collection.JavaConversions._

class S3BucketFilesSelector {

  private val BucketName = "snowplow-reevoo-unload"

  private val S3client = new AmazonS3Client(new BasicAWSCredentials(
    sys.env("S3_SNOWPLOW_BUCKET_AWS_ACCESS_KEY_ID"),
    sys.env("S3_SNOWPLOW_BUCKET_AWS_SECRET_ACCESS_KEY")
  ));

  def getRootEventFiles(folderName: String) = {
    getListOfFiles("events", folderName)
  }

  def getBadgeEventFiles(folderName: String) = {
    getListOfFiles("com_reevoo_badge_event_1", folderName)
  }

  def getConversionEventFiles(folderName: String) = {
    getListOfFiles("com_reevoo_conversion_event_1", folderName)
  }

  private def getListOfFiles(eventsFolder: String, subfolderName: String) = {
    def paginateTillEnd(objectListing: ObjectListing):List[String] = {
      val currentFileList =
        objectListing.getObjectSummaries
          .map(_.getKey)
          .filter(_.startsWith(s"${eventsFolder}/${subfolderName}"))
          .map(s"customs3://${BucketName}/" + _).toList

      if (objectListing.isTruncated)
        currentFileList ++ paginateTillEnd(S3client.listNextBatchOfObjects(objectListing))
      else currentFileList
    }
    paginateTillEnd(S3client.listObjects(BucketName, eventsFolder))
  }

}
