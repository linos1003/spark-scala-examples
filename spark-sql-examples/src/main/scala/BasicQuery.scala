import org.apache.spark.sql.SparkSession

/**
  * Created by linos on 29/04/17.
  */

object BasicQuery {


  def main(args: Array[String]): Unit = {
    val sparkSqlContext = SparkSession.builder().appName("SparkQuery basic query")
                          .master("local").getOrCreate()
    val input = sparkSqlContext.read.json("src/main/resources/cars.json")
    input.createOrReplaceTempView("Cars")
    val result = sparkSqlContext.sql("SELECT * FROM Cars")
    result.show()
  }
}
case class Cars(name: String)
