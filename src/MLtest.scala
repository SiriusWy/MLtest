
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.DenseVector
import breeze.linalg._
import breeze.plot._

object MLtest {
  def main(args: Array[String]) {
//    val ist = new MLtest
//    ist.linearRe

//    breeze_fig
  }

//  def breeze_fig {
//    val f2 = Figure()
//    f2.subplot(0) += image(DenseMatrix.rand(200,200))
//    f2.saveas("image.png")
//  }
}

class MLtest {



  def linearRe {
    val conf = new SparkConf().setAppName("linearRegression").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val training = MLUtils.loadLibSVMFile(sc, "E://2").toDF()

    val data = training.randomSplit(Array(0.7, 0.3))
    val train = data(0)
    val test = data(1)

    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.01)
      .setElasticNetParam(0)
      .setTol(0.0001)

    val time1 = System.currentTimeMillis()
    val lrModel = lr.fit(training)
    val time2 = System.currentTimeMillis()
    println("总耗时： " + (time2 - time1).toDouble / 1000 + " 秒")

//    // Print the weights and intercept for linear regression
//    println(s"Weights: ${lrModel.weights} Intercept: ${lrModel.intercept}")
//
//    // Summarize the model over the training set and print out some metrics
//    val trainingSummary = lrModel.summary
//    println(s"numIterations: ${trainingSummary.totalIterations}")
//    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
//    trainingSummary.residuals.show()
//    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//    println(s"r2: ${trainingSummary.r2}")

    val output = lrModel.transform(training)
    output.show(false)

    val newdf = output.select("label")

    // 增加一列DF 哈哈哈哈
    output.withColumn("newColName", newdf("label")).show()

    val out = output.select("rawPrediction").map {
      case Row(x: DenseVector) =>
        x.toArray(1)
    }

    val newSchema = StructType(StructField("name", DoubleType, true) :: Nil)

    val outrdd = out.map(x => Row(x))


    sqlContext.createDataFrame(outrdd, newSchema).show()


  }



}