package com.agile.bigdata

import org.apache.spark.sql.functions._

/**
  * A Word Count example using only Spark Dataframe
  */
object EmailWordCount {

  def main(args: Array[String]): Unit = {
    wordCount
  }

  /**
    * A Word Count example using only Spark Dataframe
    */
  def wordCount: Unit = {

    val df = Collector.getEmailDataFrame

    val transformedTable = df
      .select("body")
      .withColumn("body_split", split(df("body"), " "))

    val wordTable = transformedTable
      .withColumn("line", explode(transformedTable("body_split")))
      .select("line")

    val refinedTable = wordTable
      .withColumn("word", trim(wordTable("line")))
      .filter("word is not null and word != ''")

    val wordCountTable = refinedTable
      .groupBy("word")
      .agg(count(col("word")))

    wordCountTable.show(10)
  }

}
