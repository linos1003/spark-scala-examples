import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linos on 27/04/17.
  */
object MoreExamples {
  val sparkConfig = new SparkConf().setMaster("local").setAppName("More Examples")
  val sparkContext = new SparkContext(sparkConfig)

  def main(args: Array[String]): Unit = {
    averageLenghtWordContainsM()
  }

  def averageLenghtWordContainsM(): Unit = {
    val lines = sparkContext.textFile("example_text.txt")
    val words = lines.flatMap(line => line.split(" "))
    val wordsWhichContainedM = words.filter(line => line.contains("m"))
    val wordsMappedWithLength = wordsWhichContainedM.map(word => (word, word.length))
    wordsMappedWithLength.foreach(println)
    val average = wordsMappedWithLength.reduce((w1, w2) => ("mean", (w1._2 + w2._2) / 2))
    println("average length of words that contains m is : " + average)
  }


}
