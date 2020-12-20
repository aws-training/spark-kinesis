import org.apache.spark.sql.SparkSession
import model.WordCount
object SparkKinesis extends App{
  val spark = SparkSession.builder().appName("SparkKinesis").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  
  
 val streamName = "mykinesisstream"
val endPointURL = "https://kinesis.ap-south-1.amazonaws.com"
val regionName = "ap-south-1"
val AWS_ACCESS_KEY_ID = "AKIA43EB2YEDEPKBAAM2"
val SECRET_ACCESS_ID = "YocXydrjOmCj3WFfpA9xCkPYlL8BAVAmRrOMt/1/"
val checkpointInterval = 5
   import spark.implicits._   
  val lines = spark
        .readStream
        .format("kinesis")
        .option("streamName", streamName)
          .option("endpointUrl", "https://kinesis.ap-south-1.amazonaws.com")
     .option("awsAccessKeyId", AWS_ACCESS_KEY_ID)
     .option("awsSecretKey", SECRET_ACCESS_ID)
     .option("startingposition", "latest")
        .load.selectExpr("CAST(data AS STRING)").as[(String)]
  
  val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count().as[WordCount]
  wordCounts.printSchema()
  
 /*
    val cassQuery = wordCounts.writeStream.foreach(new CassandraForeachWriter[WordCount] {
                           
                          }
           ).start()       
        
*/
        
        
        
}