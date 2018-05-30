package com.agile.bigdata

import org.apache.spark.sql.functions._

/**
  * Example which count how many emails was sent
  * per hour grouped by who sent the email.
  */
object EmailCounter {

  def main(args: Array[String]): Unit = {
    countSenderPerHour
  }

  /**
    * Count how many emails was sent per hour grouped by who sent the email.
    */
  def countSenderPerHour: Unit = {
    val df = Collector
      .getEmailDataFrame

    val filteredTable = df
      .select("from", "tos", "date")
      .filter("from.address is not null and tos.address is not null")

    val transformedTable = filteredTable
      .withColumn("email_from", lower(col("from.address")))
      .withColumn("sender_hour", hour(col("date")))
      .select("email_from", "sender_hour")

    val reducedTable = transformedTable
      .groupBy("email_from", "sender_hour")
      .agg(count(col("sender_hour")))

    reducedTable.show(10)
  }

}
