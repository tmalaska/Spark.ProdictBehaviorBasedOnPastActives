package com.cloudera.sa.spark.windowing.event.cause

import java.io.{BufferedWriter, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.{Random, Date}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by ted.malaska on 11/13/14.
 */
object DataGenerator {

  val renewPercentages = Array[Double](10, 10, 10, 10, 10, 10, 10, 10, 10, 10)
  val churnPercentages = Array[Double](10, 10, 15, 2, 13, 15, 10, 10, 5, 10)
  val random = new Random
  val dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

  def main(args: Array[String]) {
    if (args.length == 0) {
      println("DataGenerator {outputDir} {NumberOfUsers} {NumberOfChurn} {NumberOfEventsPerUser} {NumberOfEventTypes} {HowManyEventBeforeDifferenceBegins}")
      println("DataGenerator output 100 50 10000 50 40 1000")
      return
    }

    val outputPath = args(0)
    val numberOfUsers = args(1).toInt
    val numberOfChurn = args(2).toInt // do this later
    val numberOfEventsPerUser = args(3).toInt
    val howManyEventsBeforeDifferenceBegins = args(4).toInt

    val fs = FileSystem.get(new Configuration)

    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputPath))))

    val startTime = (new Date).getTime

    for (i <- 0 until numberOfUsers ) {

      var eventTime = startTime
      val isChurn = (i <= numberOfChurn)

      writer.write(i + "," + dateFormat.format(new Date(eventTime)) + "," + (if (isChurn) "Churn" else "Renew") + ",000")

      for (j <- 0 until numberOfEventsPerUser) {
          //GetTime
        eventTime -= 60000 * 10 * Math.abs(random.nextGaussian())
        //GetEvent
        val eventType = getEventTypeId(j,
          howManyEventsBeforeDifferenceBegins,
          numberOfEventsPerUser, isChurn)
        writer.write(i + "," + dateFormat.format(new Date(eventTime)) + "," + eventType + "," + j)
      }
      print(".")
      if (i % 100 == 0) {
        println(";" + i)
      }
    }
  }

  def getEventTypeId(eventNumber:Int,
                      howManyEventsBeforeDifferenceBegins: Int,
                      numberOfEventsPerUser: Int,
                      isChurch: Boolean) : Int = {

    var eventTypeIndex = random.nextInt(100)

    var chancesOfEventType: Array[Double] = null

    if (eventNumber <= howManyEventsBeforeDifferenceBegins || !isChurch) {
      chancesOfEventType = renewPercentages
    } else {
      val churnPercentageNumber = (eventNumber - howManyEventsBeforeDifferenceBegins) / (howManyEventsBeforeDifferenceBegins - numberOfEventsPerUser)
      val renewPercentageNumber = 1 - churnPercentageNumber

      chancesOfEventType = Array(renewPercentages(0) * renewPercentageNumber + churnPercentages(0) * churnPercentageNumber,
          renewPercentages(1) * renewPercentageNumber + churnPercentages(1) * churnPercentageNumber,
          renewPercentages(2) * renewPercentageNumber + churnPercentages(2) * churnPercentageNumber,
          renewPercentages(3) * renewPercentageNumber + churnPercentages(3) * churnPercentageNumber,
          renewPercentages(4) * renewPercentageNumber + churnPercentages(4) * churnPercentageNumber,
          renewPercentages(5) * renewPercentageNumber + churnPercentages(5) * churnPercentageNumber,
          renewPercentages(6) * renewPercentageNumber + churnPercentages(6) * churnPercentageNumber,
          renewPercentages(7) * renewPercentageNumber + churnPercentages(7) * churnPercentageNumber,
          renewPercentages(8) * renewPercentageNumber + churnPercentages(8) * churnPercentageNumber,
          renewPercentages(9) * renewPercentageNumber + churnPercentages(9) * churnPercentageNumber
        )
    }

    for (i <- 0 until 10) {
      eventTypeIndex -= chancesOfEventType(i)
      if (eventTypeIndex <= 0){
        return i
      }
    }

    0
  }

  
}
