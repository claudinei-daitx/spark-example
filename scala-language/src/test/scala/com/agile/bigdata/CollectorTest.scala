package com.agile.bigdata

import org.scalatest.FunSuite

/**
  * Test all collector methods
  */
class CollectorTest extends FunSuite {

  test("Collector.getEmailData"){
    val emails = Collector.getEmailData
    assert(emails.hasNext)
  }

  test("Collector.getEmailDataFrame"){
    val emails = Collector.getEmailDataFrame
    assert(emails.count > 0)
  }


}
