import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linos on 27/04/17.
  */
object JoinTwoFiles {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("JoinTwoFiles").setMaster("local")
    val sparkContext= new SparkContext(sparkConf)



  }
}
