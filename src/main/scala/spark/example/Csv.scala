package spark.example

import org.apache.spark.SparkConf
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
/**
  * Created by phyrextsai on 2016/6/7.
  */


object Csv {

  def main(args: Array[String]){
    val start = java.lang.System.currentTimeMillis()
    println("start : " + start)
    var conf = new SparkConf().setAppName("ColumnDisplay").setMaster("local")

    var sc = new SparkContext(conf)

    val csv = sc.textFile("Scorecard_small.csv", 2).cache

    // show heaer of CSV
    showCSVHeader(csv)

    // show data of CSV
    showCSVData(csv)

    // query from CSV
    queryCSV(sc)

    val end = java.lang.System.currentTimeMillis()
    println("end : " + end)
  }

  def showCSVData(csv: RDD[String]) : Unit = {
    val rows = csv.map(line => line.split(","))
    var rowCount = 0;
    for (row <- rows) {
      rowCount += 1;
      if (rowCount > 1) {
        var columnCount = 0;
        for (data <- row) {
          println("data[" + columnCount + "] : " + data)
          columnCount += 1;
        }
      }
    }
  }

  def showCSVHeader(csv: RDD[String]) : Unit = {
    val rows = csv.map(line => line.split(","))
    var columnCount = 0;
    val columns = rows.first()
    columns.foreach(column => {
      println("column[" + columnCount + "] : " + column)
      columnCount += 1
    })
  }

  def queryCSV(sc: SparkContext): Unit = {
    var sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val csvAll = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .load("Scorecard_small.csv")

    csvAll.select("Id", "UNITID", "OPEID", "opeid6", "INSTNM", "CITY", "STABBR", "ZIP", "AccredAgency").foreach(
      println
    )
  }
}
