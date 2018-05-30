package com.agile.bigdata


import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Example which create a Spark Graph
  * to apply Social Network Analysis (SNA)
  * from your email.
  */
object EmailSNA {
  def main(args: Array[String]): Unit = {
    getSocialNetworkAnalysis
  }

  /**
    * Create a Spark Graph which is
    * possible to apply Social Network Analysis (SNA)
    * from your email.
    */
  def getSocialNetworkAnalysis: Unit = {

    val spark = SparkUtil.getSession
    val df = Collector.getEmailDataFrame

    val filteredTable = df
      .filter("from.address is not null and tos.address is not null")
      .select("from", "tos")

    val transformedTable = filteredTable
      .withColumn("email_from", lower(col("from.address")))
      .withColumn("email_base", explode(col("tos.address")))
      .withColumn("email_to", lower(col("email_base")))
      .filter("email_from is not null and email_to is not null")
      .select("email_from", "email_to")
      .distinct()
    transformedTable.createOrReplaceTempView("transformed_table")

    val verticeTable = transformedTable
      .select("email_from")
      .union(
        transformedTable
          .select("email_to")
      )
      .distinct
      .withColumn("vertex_id", row_number() over Window.orderBy("email_from"))
    verticeTable.createOrReplaceTempView("vertice_table")

    val edgeTable = spark
      .sql("select " +
        " cast(vf.vertex_id as long) as vertex_id_from," +
        " cast(vt.vertex_id as long) as vertex_id_to," +
        " 'sent' as relationship" +
        " from transformed_table as t " +
        " left join vertice_table vf on t.email_from = vf.email_from " +
        " left join vertice_table vt on t.email_to = vt.email_from " +
        " ")
    val vertexRDD: RDD[(VertexId, String)] = verticeTable
      .select("vertex_id", "email_from")
      .rdd
      .map(x => (
        x.get(0).toString.toLong,
        x.get(1).toString
      )
      )
      .asInstanceOf[RDD[(VertexId, String)]]

    val edgeRDD: RDD[Edge[String]] = edgeTable
      .rdd
      .map(x => Edge(
        x.get(0).toString.toLong,
        x.get(1).toString.toLong,
        x.get(2).toString
      )
      )
      .asInstanceOf[RDD[Edge[String]]]

    val graph = Graph(vertexRDD, edgeRDD, "not sent")

    val facts: RDD[String] = graph
      .triplets
      .map(triplet =>
        s"A email from ${triplet.srcAttr} was ${triplet.attr} to ${triplet.dstAttr}."
      )
    facts
      .collect
      .foreach(println(_))
  }
}
