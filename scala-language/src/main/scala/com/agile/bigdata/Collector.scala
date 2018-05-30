package com.agile.bigdata

import java.io.File

import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.spark.sql.DataFrame

import scala.io.Source.fromFile

/**
  * Read the emails in the Avro file and transform to a structured form.
  */
object Collector {

  val avroPath = "/tmp/email/emails.avro"

  /**
    * Get the emails from Avro file.
    *
    * @return a DataFileReader with the emails
    */
  def getEmailData: DataFileReader[GenericRecord] = {
    val avroSchemaPath = "/tmp/email/email.avro.schema"
    val schema = fromFile(avroSchemaPath).mkString
    val schemaParser = new Schema.Parser
    val schemaObject = schemaParser.parse(schema)
    val readerSchema = new GenericDatumReader[GenericRecord](schemaObject)

    val myFile = new File(avroPath)
    return new DataFileReader[GenericRecord](myFile, readerSchema)
  }

  /**
    * Get the emails from Avro file.
    *
    * @return a Spark dataframe with the emails.
    */
  def getEmailDataFrame: DataFrame = {
    SparkUtil
      .getSession
      .read
      .avro(avroPath)
  }


}
