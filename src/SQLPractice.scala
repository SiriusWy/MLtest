import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/11/1.
 */
object SQLPractice {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

//  dataframe的操作

//    val df = sqlContext.read.json("E:/people.json")
//    df.select(df("age")+1, df("name") ).show()
//    df.filter(df("age") > 22).show()
//    df.groupBy(df("age")).count().show()

    // RDD(LabeledPoint) = (label : String, features : Vector )
    val datardd = MLUtils.loadLibSVMFile(sc, "E:/2")
//    datardd.foreach(println)

    val data = MLUtils.loadLibSVMFile(sc, "E:/2").toDF("a","b")
    data.show(30, false)
//    data.foreach(println)
    data.registerTempTable("wytable")

    val wytabel = sqlContext.sql("select * from wytable")
    wytabel


    val newrdd = Array("1,177.3,wangyue", "0,158.1,lichenxi",
      "0,160.5,zhangruiying", "1,175.9,wangquanzhen",
      "2,220.1,Bogote", "2,250.2,goblin")
    val rdd = sc.makeRDD(newrdd)
    val rddDF = rdd.toDF("name") // 这种RDD,spark没有解析格式，所以只有一列
//    rddDF.groupBy("").count().show()
    val schemaString = "sex heigh name"
    val schema = StructType(Array(StructField("sex",IntegerType,true), StructField("heigh",DoubleType,true), StructField("name",StringType,true)))

    val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0).toInt, p(1).toDouble, p(2).trim))

    val homeDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    homeDataFrame.show()
    println(homeDataFrame.schema)
    homeDataFrame.registerTempTable("homeTable")
    sqlContext.sql("select heigh from homeTable where heigh>160 and sex=1").show()
//    sqlContext.sql("select heigh from homeTable group by sex").show()
    sqlContext.sql("select sex as SEX, avg(heigh) as totalHeigh from homeTable group by sex").show()
//// 用case class定义schema
//
//    case class Person(name: String, age: Int)
//    /**
//    * Read a text file from HDFS, a local file system (available on all nodes), or any
//      * Hadoop-supported file system URI, and return it as an RDD of Strings.
//    */
//    val people = sc.textFile("E:/person.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
//
//    people.foreach(print)




    sc.stop()
  }

}

