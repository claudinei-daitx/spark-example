package com.agile.bigdata

import org.scalatest.FunSuite

/**
  * Test all email methods.
  */
class EmailTest extends FunSuite {
  test("EmailCounter.countSenderPerHour") {
    EmailCounter.countSenderPerHour
  }

  test("EmailSNA.getSocialNetworkAnalysis") {
    EmailSNA.getSocialNetworkAnalysis
  }

  test("EmailTimeSeries.getTimeSeries") {
    EmailTimeSeries.getTimeSeries()
  }

  test("EmailWordCount.wordCount") {
    EmailWordCount.wordCount
  }

}
