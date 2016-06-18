package spark.example.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.configuration.Strategy._

/**
  * Created by phyrextsai on 2016/6/10.
  */
object RandomForestClassification {
  def main(args: Array[String]): Unit = {
    val dataset = "YearPredictionMSD"
    val conf = new SparkConf().setAppName("RandomForestClassificatione").setMaster("local")
    val sc = new SparkContext(conf)
    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "train_2.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 9
    val categoricalFeaturesInfo = Map[Int, Int](/*0 -> 0, 1 -> 1, 2 -> 2, 5 -> 5, 8 -> 8, 13 -> 13, 15 -> 15, 19 -> 19, 30 -> 30, 37 -> 37, 58 -> 58, 59 -> 59*/) // Map storing arity of categorical features. E.g., an entry (n -> k) indicates that feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.
    val numTrees = 100 // Use more in practice.
    val featureSubsetStrategy = "sqrt" // Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird". If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest) set to "sqrt".
    val impurity = "entropy" // Criterion used for information gain calculation. Supported values: "gini" (recommended) or "entropy".
    val maxDepth = 4 // Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. (suggested value: 4)
    val maxBins = 10 // maximum number of bins used for splitting features (suggested value: 100)


    /*val model = RandomForest.trainClassifier(trainingData,
      Strategy.defaultStrategy("Classification"),
      3, featureSubsetStrategy, 1)*/

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, "target/tmp/" + dataset)
    val sameModel = RandomForestModel.load(sc, "target/tmp/" + dataset)
    // $example off$
  }
}
