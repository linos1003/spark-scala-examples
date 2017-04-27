import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
  * Created by linos on 26/04/17.
  */
object WordCount {

  //    StreamingExamples.setStreamingLogLevels()
  def main(args:Array[String]) {
    println("Hello world")
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("localhost")
    // Create the context
    val ssc = new SparkContext(sparkConf);

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFile("/home/linos/Documents/example_text.txt");
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
   println( "the word count of "  +  wordCounts)
  }

}
