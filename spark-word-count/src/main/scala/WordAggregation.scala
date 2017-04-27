import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linos on 27/04/17.
  */
object WordAggregation {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordAggregation").setMaster("local");
    val sc= new SparkContext(sparkConf)

    val rddList= sc.parallelize(Array(("saprk",3),("hadoop",5),("yarn",3),("saprk",1),("hadoop",2),("yarn",4)))
     val aggregatedWords= rddList.aggregateByKey(0)((k,v) => v.toInt+k, (v,k) => k+v).collect
    println("result of aggregate By Key" + aggregatedWords.toList)

  }
}
