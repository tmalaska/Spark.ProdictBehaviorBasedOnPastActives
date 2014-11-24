package com.cloudera.sa.spark.windowing.event.cause

import java.text.SimpleDateFormat

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object AnalyzeDecisionPointTimeLineMain {
  def main(args: Array[String]) {
    if (args.length == 0) {
      println("AnalyzeDecisionPointTimeLineMain {inputDir} {rootOutputDir} {numberOfPartitions} {excelTimeInternalInSeconds} {excelNumberOfIntervals} {inflectionPointEventType} {timeUnitOfAgingImportanceInSeconds}")
      println("AnalyzeDecisionPointTimeLineMain null output 2 60 100 Churn,Renew 4")
      return
    }

    val dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

    val inputDir = args(0)
    val outputDir = args(1)
    val numberOfPartitions = args(2).toInt
    val excelTimeInternal = args(3).toInt * 1000
    val excelNumberOfIntervals = args(4).toInt
    val inflectionPointEventTypes = args(5).split(",")
    val timeUnitOfAgingImportance = args(6).toInt * 1000

    val sparkConf = new SparkConf().setAppName("AnalyzeDecisionPointTimeLineMain: " + args(0))
    sparkConf.set("spark.cleaner.ttl", "120000");

    val sc = new SparkContext(sparkConf)

    var dataFile: RDD[String] = null

    if (!inputDir.equals("null")) {
      dataFile = sc.hadoopFile("/tmp", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1).
        map(t => t._2.toString)
    } else {
      //user,time,eventType,extra
      dataFile = sc.parallelize(Array(
        "100,12/01/14 9:00:00,Call,120",
        "100,12/01/14 9:03:00,Disconnect,120",
        "100,12/01/14 9:06:00,Call,120",
        "100,12/01/14 9:10:00,Call,120",
        "100,12/01/14 9:11:00,Disconnect,120",
        "100,12/01/14 9:13:00,Call,120",
        "100,12/01/14 9:15:00,Call,120",
        "100,12/01/14 9:20:00,Call,120",
        "100,12/01/14 9:23:00,Disconnect,120",
        "100,12/01/14 9:24:00,BadService,120",
        "100,12/01/14 9:26:00,Call,120",
        "100,12/01/14 9:25:00,Churn,120",
        "101,12/01/14 9:00:00,Call,120",
        "101,12/01/14 9:03:00,Disconnect,120",
        "101,12/01/14 9:06:00,Call,120",
        "101,12/01/14 9:10:00,Call,120",
        "101,12/01/14 9:11:00,Disconnect,120",
        "101,12/01/14 9:13:00,Call,120",
        "101,12/01/14 9:15:00,Call,120",
        "101,12/01/14 9:20:00,Call,120",
        "101,12/01/14 9:23:00,Call,120",
        "101,12/01/14 9:24:00,Call,120",
        "101,12/01/14 9:26:00,Call,120",
        "101,12/01/14 9:25:00,Renew,120",
        "102,12/01/14 9:00:00,Call,120",
        "102,12/01/14 9:03:00,Call,120",
        "102,12/01/14 9:06:00,Call,120",
        "102,12/01/14 9:10:00,Call,120",
        "102,12/01/14 9:11:00,Disconnect,120",
        "102,12/01/14 9:13:00,Call,120",
        "102,12/01/14 9:15:00,Call,120",
        "102,12/01/14 9:20:00,Call,120",
        "102,12/01/14 9:23:00,Call,120",
        "102,12/01/14 9:24:00,Call,120",
        "102,12/01/14 9:26:00,Call,120",
        "102,12/01/14 9:25:00,Renew,120")
      )
    }

    //This will format and get ready for key operations
    val eventParts = dataFile.mapPartitions(it => {
      val dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

      it.map(r => {
        val s = r.split(",")
        val s1 = s(1)

        val reverseTime = Long.MaxValue - dateFormat.parse(s(1)).getTime

        (s(0) + "," + reverseTime, new EventPart(s(2), s(3)))
      })
    })

    val partitioner = new Partitioner {
      override def numPartitions: Int = numberOfPartitions

      override def getPartition(key: Any): Int = {

        val str = key.toString
        val userId = str.substring(0,str.indexOf(','))

        Math.abs(userId.hashCode() % numPartitions)
      }
    }

    //eventParts.partitionBy(partitioner)

    val partedSortedRDD = new ShuffledRDD[String, EventPart, EventPart](eventParts, partitioner).
      setKeyOrdering(implicitly[Ordering[String]].reverse)

    println("-----------")
    println("partedSortedRDD:" + partedSortedRDD.count())


    val windowedRDD = partedSortedRDD.mapPartitions(it => {
      windowingProcess(excelTimeInternal, excelNumberOfIntervals, timeUnitOfAgingImportance, inflectionPointEventTypes, it)
    })

    println("-----------")
    println("windowedRDD:" + windowedRDD.count())

    windowedRDD.take(100).foreach(t => {
      //println("t.eventCountsAndWeights.size:" + t.eventCountsAndWeights.size);
      t.eventCountsAndWeights.foreach(e => println(" - " + e._1 + ":" + e._2._1.value + "," + e._2._2.value))
      //println("t.eventCountsInATimeLine.size: " + t.eventCountsInATimeLine.size)


    })


    val excelTimeLine = windowedRDD.flatMap(t => {
      val sortedSet = new mutable.TreeSet[String]
      val timeLineMap = new mutable.HashMap[String, StringBuilder]

      //1. populate the sortedSet
      t.eventCountsAndWeights.foreach(r => {
        sortedSet.+=(r._1)
        timeLineMap.+=((r._1, new StringBuilder))
      })

      t.eventCountsInATimeLine.foreach(i => {
        timeLineMap.foreach(entity => {
          val eventTypeCount = i.get(entity._1)
          if (eventTypeCount.isEmpty) {
            entity._2.append("0,")
          } else {
            entity._2.append(eventTypeCount.get.value + ",")
          }
        })
      })

      val results = new Array[String](timeLineMap.size)
      var index = 0;
      timeLineMap.foreach(entity => {
        results(index) = t.userId + "," + entity._1 + "," + entity._2.toString
        index += 1
      })

      results
    })

    println("-----------")
    println("excelTimeLine:" + excelTimeLine.count())

    val excelEventCountsAndWeightsRDD = windowedRDD.flatMap(t => {

      val size = t.eventCountsAndWeights.size

      val results = new Array[String](size)

      var index = 0;
      t.eventCountsAndWeights.foreach(entity => {
        results(index) = t.userId + "," + entity._1 + "," + entity._2._1.value + "," + entity._2._2.value
        index += 1
      })
      results
    })

    println("-----------")
    println("excelEventCountsAndWeightsRDD:" + excelEventCountsAndWeightsRDD.count())

    excelEventCountsAndWeightsRDD.saveAsTextFile(outputDir + "/EventCountsAndWeights")
    excelTimeLine.saveAsTextFile(outputDir + "/TimeLine")

  }

  def windowingProcess(excelTimeInternal:Int,
                   excelNumberOfIntervals:Int,
                   timeUnitOfAgingImportance:Int,
                   inflectionPointEventTypes:Array[String],
                   it:Iterator[(String,EventPart)] ): Iterator[userWindow] = {
    var currentKey = "null"
    var rr: userWindow = null
    val ml = new mutable.MutableList[userWindow] // = new mutable.Seq[util.HashMap] {}
    var interval = -1
    var currentHashMap: mutable.HashMap[String, LongCounter] = null
    var hitInflectionPoint = false
    var inflectionPointTime:Long = -1

    //Iterator through all the values
    it.foreach(r => {

      //Parse the values
      val str = r._1.toString
      val indexOfComma = str.indexOf(',')
      val userId = str.substring(0, indexOfComma)
      val eventTime = str.substring(indexOfComma + 1).toLong + Long.MaxValue

      //println("row:" + str + ", eventTime:" + eventTime + ", eventType" + r._2.eventId)

      //Are we still on the same key?
      if (!userId.equals(currentKey)) {
        currentKey = userId
        if (rr != null) {
          ml.+=(rr)
        }
        rr = new userWindow(userId, new mutable.HashMap[String, (LongCounter, DoubleCounter)],
          new mutable.MutableList[mutable.HashMap[String, LongCounter]])


        rr.eventCountsInATimeLine+=(new mutable.HashMap[String, LongCounter]())

        interval = -1
        hitInflectionPoint = false
        inflectionPointTime = -1
      }


      //println("hitInflectionPoint:" + hitInflectionPoint )
      if (hitInflectionPoint) {

        //Convert our time into intervals
        println("diffTime:" + eventTime + "-" + inflectionPointTime )
        val diffTime =  eventTime - inflectionPointTime

        //If time is not in this range then throw it out
        //println("-diffTime:" + diffTime)
        if (diffTime >= 0) {
          val desiredInterval = Math.ceil(diffTime / excelTimeInternal)

          //Up the current Interval until we make enough
          //println("--desiredInterval:" + desiredInterval)
          //println("--interval:" + interval)
          //println("--excelNumberOfIntervals:" + excelNumberOfIntervals)
          while (interval < desiredInterval && interval <= excelNumberOfIntervals) {
            interval += 1
            currentHashMap = new mutable.HashMap[String, LongCounter]()
            rr.eventCountsInATimeLine+=(currentHashMap)
            //println("---rr.eventCountsInATimeLine:" + rr.eventCountsInATimeLine.size )
          }

          //Make sure we got to the desiredInterval
          if (desiredInterval == interval) {

            //1. Do the time line
            //Add or update the counter in the HashMap
            val eventEntity = currentHashMap.get(r._2.eventId)
            if (eventEntity.isEmpty) {
              currentHashMap.+=((r._2.eventId, new LongCounter(1)))
            } else {
              eventEntity.get.value += 1
            }

            //2. Do the event counting and weighing
            val eventCountAndWeight = rr.eventCountsAndWeights.get(r._2.eventId)
            if (eventCountAndWeight.isEmpty) {

              rr.eventCountsAndWeights.+=((r._2.eventId,
                (new LongCounter(1),
                  new DoubleCounter(calcWeight(timeUnitOfAgingImportance, inflectionPointTime, eventTime)))))
              //println("---rr.eventCountsAndWeights:" + rr.eventCountsAndWeights.size)
            } else {
              eventCountAndWeight.get._1.value += 1
              eventCountAndWeight.get._2.value += calcWeight(timeUnitOfAgingImportance, inflectionPointTime, eventTime)
            }

          } else {
            //nothing
          }
        } else {
          //nothing
        }
      } else if (inflectionPointEventTypes.contains(r._2.eventId)) {
        //println("Inflection")
        hitInflectionPoint = true
        inflectionPointTime = eventTime
      } else {
        //println("NonInflection:" + r._2.eventId + "\n")
      }
    })
    if (rr != null) {
      ml.+=(rr)
    }
    ml.iterator
  }

  //Update this
  def calcWeight(timeUnitOfAgingImportance:Int, inflectionPointTime:Long, eventTime:Long):Double = {
    val gapTime = eventTime - inflectionPointTime
    val gapInterval = gapTime/timeUnitOfAgingImportance

    gapTime / (1 + 0.65 * gapInterval)
  }

  //(mutable.HashMap[String, (LongCounter, DoubleCounter)],mutable.MutableList[mutable.HashMap[String, LongCounter]])
  class userWindow (val userId:String,
                     val eventCountsAndWeights: mutable.HashMap[String, (LongCounter, DoubleCounter)],
                    val eventCountsInATimeLine: mutable.MutableList[mutable.HashMap[String, LongCounter]]
                     )  extends Serializable {}


  class LongCounter (var value:Long)  extends Serializable {}

  class DoubleCounter (var value:Double)  extends Serializable {}

  class EventPart (val eventId: String,
                  val content: String
                  ) extends Serializable {}

  class EventTimePart (
                 val time: Long,
                 val eventId: String,
                 val content: String
                 )  extends Serializable {}
}