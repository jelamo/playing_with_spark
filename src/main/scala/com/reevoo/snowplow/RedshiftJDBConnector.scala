package com.reevoo.snowplow

import java.util.Properties
import java.sql.DriverManager

object RedshiftJDBConnector {

  final val DbUrl = sys.env("TARGET_REDSHIFT_DB_URL")

  final val DriverClass = "com.amazon.redshift.jdbc41.Driver"

  final val DefaultDestinationTable = "historical_mark_events"

  def connectionProperties = {
    val props = new Properties()
    props.setProperty("driver", DriverClass)
    props.setProperty("user", sys.env("TARGET_REDSHIFT_DB_USER"))
    if (sys.env.get("TARGET_REDSHIFT_DB_PASSWORD").getOrElse("") != "") {
      props.setProperty("password", sys.env("TARGET_REDSHIFT_DB_PASSWORD"))
    }
    props
  }

  def createOrTruncateMarkEventsTable(destinationTable: String, overwrite: Boolean) = {
    Class.forName(DriverClass)
    val connection = DriverManager.getConnection(DbUrl, connectionProperties)
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"CREATE TABLE IF NOT EXISTS $destinationTable (" +
        "dvce_created_tstamp   TIMESTAMP," +
        "event_id              VARCHAR(36)," +
        "network_userid        VARCHAR(38)," +
        "domain_sessionid      VARCHAR(36)," +
        "trkref                VARCHAR(20)," +
        "event_type            VARCHAR(20)," +
        "reviewable_context    VARCHAR(4096)," +
        "additional_properties VARCHAR(4096)," +
        "content_type          VARCHAR(19)," +
        "hit_type              VARCHAR(14)," +
        "cta_page_use          VARCHAR(17)," +
        "cta_style             VARCHAR(40)," +
        "implementation        VARCHAR(14))")

    if (overwrite) statement.executeUpdate(s"TRUNCATE TABLE $destinationTable")
  }

}
