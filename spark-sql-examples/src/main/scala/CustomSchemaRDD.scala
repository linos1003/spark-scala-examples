import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by linos on 29/04/17.
  */
object CustomSchemaRDD {
  val sparkConfig = new SparkConf().setMaster("local")
    .setAppName("SparkQuery Custom Schema")
    .set("spark.driver.allowMultipleContexts", "true")
  val sqlContext = SparkSession.builder().config(sparkConfig).getOrCreate()
  val sc = sqlContext.sparkContext
  val schemaOptions = Map("header" -> "true", "inferSchema" -> "true")


  def main(args: Array[String]) {

    // Create an RDD
    val fruit = sc.textFile("src/main/resources/fruits.txt")

    // The schema is encoded in a string
    val schemaString = "id name"
    // Generate the schema based on the string of schema
    val schema = StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    schema.foreach(println)
    // Convert records of the RDD (fruit) to Rows.
    val rowRDD = fruit.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    rowRDD.foreach(println)

    // Apply the schema to the RDD.
    val fruitDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    fruitDataFrame.show()

    // Register the DataFrames as a table.
    fruitDataFrame.createOrReplaceTempView("fruit")
    /**
      * SQL statements can be run by using the sql methods provided by sqlContext.
      */
    val results = sqlContext.sql("SELECT * FROM fruit")
    results.show()

  }
}