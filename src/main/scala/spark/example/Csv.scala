package spark.example

import org.apache.spark.SparkConf
import java.util.Calendar
import org.apache.spark.SparkContext
/**
  * Created by phyrextsai on 2016/6/7.
  */


object Csv {

  def main(args: Array[String]){
    val start = java.lang.System.currentTimeMillis()
    println("start : " + start)
    var conf = new SparkConf().setAppName("ColumnDisplay").setMaster("local")

    var sc = new SparkContext(conf)

    val csv = sc.textFile("Scorecard_small.csv", 2).cache()

    val rows = csv.map(line => line.split(","))

    val columns = rows.first()
    columns.foreach(column => println("column : " + column))

    val numRows = rows.count()
    println("numRows : " + numRows)
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

    queryCSV(sc)

    val end = java.lang.System.currentTimeMillis()
    println("end : " + end)
  }

  def queryCSV(sc: SparkContext): Unit = {
    var sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val csvAll = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      //.option("inferSchema", "true") // Automatically infer data types
      .load("Scorecard_small.csv")
    //sqlContext.load("com.databricks.spark.csv", Map("path" -> "Scorecard_small.csv", "header" -> "true"))
    //csvAll.registerTempTable("AllData")
    /*val result = sqlContext.sql("select Id, UNITID, OPEID, opeid6, INSTNM, CITY, STABBR, ZIP, AccredAgency from AllData").rdd.map(row => row.toString.substring(1, row.length - 2))

    result.foreach {
      println
    }*/
    csvAll.select("Id", "UNITID", "OPEID", "opeid6", "INSTNM", "CITY", "STABBR", "ZIP", "AccredAgency").foreach(
      println
    )
  }
}
