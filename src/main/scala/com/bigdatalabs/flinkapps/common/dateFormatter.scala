package com.bigdatalabs.flinkapps.common

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

object dateFormatter {

  val DATE_FORMAT = "yyyy-MM-dd"

  def convertStringToDate(s: String): Date = {
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    dateFormat.parse(s)
  }

  def extractYr(d: Date): Integer= {
    val dateFormat3 = new SimpleDateFormat("YYYY")
    dateFormat3.format(d).toInt
  }

}

