import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linos on 28/04/17.
  */
object JsonWithSpark {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("More Examples")
    val sparkContext = new SparkContext(sparkConfig)
    val sqlContext = new SQLContext(sparkContext)
    val path = "jsonFile"
    val df = sqlContext.read.json(path)
    df.show()
    df.select("dict").show()
  }
}
