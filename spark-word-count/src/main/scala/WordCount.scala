import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by linos on 26/04/17.
  */
object WordCount {


  def main(args: Array[String]) {
    println("Hello world")
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local")

    // Create the context
    val ssc = new SparkContext(sparkConf);
    //Create an RDD from texte file
    val lines = ssc.textFile("/home/linos/Documents/example_text.txt");
    // Create a list of words using flatMap
//    val words = lines.flatMap(word=>word.split(" "))
    val words = lines.flatMap(_.split(" "))
    //1-Map each word with 1 and aggregate using reduceByKey
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val wordCountWithAggregate = words.map(x=> (x,1)).aggregateByKey(0)((k,v) => v.toInt+k, (v,k) => k+v).collect
    println("the word count of " + wordCounts)
    println("the word count of aggregate " + wordCountWithAggregate.toList)
  }

}
