package spark.example

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by phyrextsai on 2016/6/17.
  */
object SparkSQL {
  def main(args: Array[String]){
    var conf = new SparkConf().setAppName("ColumnDisplay").setMaster("local")

    var sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val data = MLUtils.loadLibSVMFile(sc, "YearPredictionMSD.t")

    //println(data.cache().first().label)

    println("count : " + data.cache().count())

    println(data.cache().map((f) => {f.label - 1922}).foreach(println))

    //println("numClasses : " + data.cache().map((f) => { (f.label - 1922).toInt + "  " + f.toString() }).max())





  }
}
