
package spark.example.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint


object LinearSupportVectorMachines {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearSupportVectorMachines").setMaster("local")
    val sc = new SparkContext(conf)
    // Load training data in LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc, "train_3.LINE.txt")

    // Split data into training (90%) and test (10%).
    val splits = data.randomSplit(Array(0.9, 0.1), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
//    val svmAlg = new SVMWithSGD()
//    svmAlg.optimizer.
//      setNumIterations(20).
//      setRegParam(0.1).
//      setUpdater(new L1Updater)
//
//   val model = svmAlg.run(training)
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

  }
}
// scalastyle:on println
