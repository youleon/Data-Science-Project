package spark.example

import org.apache.spark.SparkConf
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Set
/**
  * Created by phyrextsai on 2016/6/7.
  * Edit by jesswang on 2016/06/07
  */


object Csv {

  // the column which is not contains null data
  var columnSet : Set[String] = Set()

  def main(args: Array[String]){
    val start = java.lang.System.currentTimeMillis()
    println("start : " + start)
    var conf = new SparkConf().setAppName("ColumnDisplay").setMaster("local")

    var sc = new SparkContext(conf)

    val csv = sc.textFile("Scorecard_small.csv", 2).cache

    // show heaer of CSV
    val columnMap : Map[String, String] = showCSVHeader(csv)

    // show data of CSV
    showCSVData(csv, columnMap)

    // query from CSV
    queryCSV(sc)

    val end = java.lang.System.currentTimeMillis()
    println("end : " + end)
    sc.stop()
  }

  def showCSVData(csv: RDD[String], columnMap: Map[String, String]) : Unit = {
    val rows = csv.map(line => line.split(","))
    var rowCount = 0
    for (row <- rows) {
      rowCount += 1
      if (rowCount > 1) {
        var columnCount = 0
        for (data <- row) {
         // println("data[" + columnCount + "] : " + data)
          columnCount += 1
          if (data != null && data.trim != "") {
            val key = columnCount + ""
            val column = columnMap.get(key).getOrElse(null)
            //println(column)
            if (column != null) {
              //println("key : " + key + "," + column)
              columnSet += column
              //println("#" + columnSet)
            }
          }
        }
      }
    }
  }

  def showCSVHeader(csv: RDD[String]) : Map[String, String] = {
    var columnMap :Map[String, String] = Map()
    val rows = csv.map(line => line.split(","))
    var columnCount = 0
    val columns = rows.first()
    columns.foreach(column => {
      //println("column[" + columnCount + "] : " + column)
      columnCount += 1
      val key : String = columnCount + ""
      columnMap += (key -> column)
    })

    columnMap
  }

  def queryCSV(sc: SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val csvAll = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferScheme", "true")
      .load("Scorecard.csv")

    csvAll.registerTempTable("records")

    var count = 0
    var str = ""

    columnSet.foreach((c) => {
      if(count > 0){
        str += " ," + c
      }else{
        str += c
      }
      count += 1
    })

    str = "s11.INSTNM , s11.CONTROL, s11.mn_earn_wne_p6, s11.md_earn_wne_p6, s11.pct10_earn_wne_p6, s11.pct25_earn_wne_p6, s11.pct75_earn_wne_p6, s11.pct90_earn_wne_p6"

    // put column in the select
    val records = sqlContext.sql("SELECT "+str+" FROM records s11 inner join  records s13 ON s11.UNITID=s13.UNITID  WHERE s11.Year=2011 AND s13.Year=2013   AND s11.pct75_earn_wne_p6 IS NOT NULL  AND s11.pct75_earn_wne_p6 != 'PrivacySuppressed'  and s11.PREDDEG like 'Predominantly bachelor%s-degree granting'  AND s13.CCBASIC NOT LIKE '%Special%' and s11.md_earn_wne_p6 > 0 ORDER BY s11.pct75_earn_wne_p6 DESC ")
    println(records.count())

    //records.foreach(println)


  }
}
