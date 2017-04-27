import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linos on 27/04/17.
  */
object Aggregation {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordAggregation").setMaster("local");
    val sc = new SparkContext(sparkConf)

    val rddList = sc.parallelize(Array(("saprk", 3), ("hadoop", 5), ("yarn", 3), ("saprk", 1), ("hadoop", 2), ("yarn", 4)))
    val aggregatedWords = rddList.aggregateByKey(0)(_ + _, _ + _).collect
    println("result of aggregate By Key" + aggregatedWords.toList)


    val initialCount = 0;
    val addToSum = (n: Int, v: Int) => math.max(n , v)
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = rddList.aggregateByKey(initialCount)(addToSum, sumPartitionCounts)
    println("result of aggregate By Key:" )
    countByKey.foreach(println)


  }
}
