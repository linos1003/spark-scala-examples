import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linos on 29/04/17.
  */

object BasicQuery {


  def main(args: Array[String]): Unit = {
    val sparkSqlContext = SparkSession.builder().appName("SparkQuery basic query")
                                                .master("local")
                                                .getOrCreate()



    val input = sparkSqlContext.read.json("src/main/resources/cars1.json")
    input.createOrReplaceTempView("Cars1")

    val result = sparkSqlContext.sql("SELECT * FROM Cars1")

    result.show()
  }
}
