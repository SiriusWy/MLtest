import org.apache.spark.ml.classification.{NaiveBayes, LogisticRegression, OneVsRest}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Created by Administrator on 2016/12/4.
 */
object OneVSall {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("linearRegression").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // parse data into dataframe
    val data = MLUtils.loadLibSVMFile(sc, "e:\\3")
    val Array(train, test) = data.toDF().randomSplit(Array(0.7, 0.3))

    // instantiate multiclass learner and train
    val ovr = new OneVsRest().setClassifier(new LogisticRegression())

    val ovrModel = ovr.fit(train)

    // score model on test data
    val predictions = ovrModel.transform(test).select("prediction", "label")
    predictions.show()
    val predictionsAndLabels = predictions.map {case Row(p: Double, l: Double) => (p, l)}

    // compute confusion matrix
    val metrics = new MulticlassMetrics(predictionsAndLabels)
    println(metrics.confusionMatrix)

    // the Iris DataSet has three classes
    val numClasses = 3

    println("label\tfpr\n")
    (0 until numClasses).foreach { index =>
      val label = index.toDouble
      println(label + "\t" + metrics.falsePositiveRate(label))
    }
  }
}
