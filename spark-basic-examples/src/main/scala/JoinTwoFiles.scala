import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by linos on 27/04/17.
  */
object JoinTwoFiles {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("JoinTwoFiles").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val df1 = sqlContext.load("com.databricks.spark.csv", Map("path" -> "f1.csv", "header" -> "true"))
    val df2 = sqlContext.load("com.databricks.spark.csv", Map("path" -> "f2.csv", "header" -> "true"))
    df1.rdd.collect().foreach(println)
    df2.rdd.collect().foreach(println)
    df1.join(df2, df1("type") <=> df2("type"), "inner").show()

  }
}
