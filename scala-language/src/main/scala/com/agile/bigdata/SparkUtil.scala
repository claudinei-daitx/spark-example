package com.agile.bigdata

import org.apache.spark.sql.SparkSession

/**
  * Spark utility class
  */
object SparkUtil {
  /**
    * Get a Spark Session
    *
    * @return a Spark Session
    */
  def getSession: SparkSession = {
    val spark = SparkSession
      .builder
      .appName("my spark application name")
      .master("local")
      .getOrCreate

    return spark
  }


}
