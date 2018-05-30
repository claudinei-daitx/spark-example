package com.agile.bigdata

object EmailTimeSeries {

  def main(args: Array[String]): Unit = {
    getTimeSeries("myemail@gmail.com")
  }

  /**
    * Create a dataframe which show the amount
    * of received and sent emails per day.
    *
    * @param email your email. Ex: myemail@myemail.com
    */
  def getTimeSeries(email: String = "myemail@myemail.com"): Unit = {
    val spark = SparkUtil.getSession
    val df = Collector.getEmailDataFrame

    val filteredTable = df
      .select("from", "tos", "date")
      .filter("from.address is not null and tos.address is not null")

    filteredTable
      .createOrReplaceTempView("filtered_table")

    val transformedTable = spark
      .sql(" select " +
        " cast(to_date(date) as timestamp) as email_timestamp," +
        s" (case when lower(from.address) = '${email.toLowerCase}' then 1 else 0 end) as email_from," +
        s" (case when lower(from.address) != '${email.toLowerCase}' then 1 else 0 end) as email_to" +
        " from filtered_table "
      )

    transformedTable
      .createOrReplaceTempView("transformed_table")

    val groupedTable = spark
      .sql(" select " +
        " email_timestamp," +
        " day(email_timestamp) as email_day, " +
        s" sum(email_from) as total_email_sent, " +
        s" sum(email_to) as total_email_received " +
        " from transformed_table " +
        " group by email_timestamp ")

    groupedTable.show(10)
  }

}
