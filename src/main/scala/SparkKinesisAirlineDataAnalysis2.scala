import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import model.{Flight,WordCount}
object SparkKinesisAirlineDataAnalysis2 extends App{
  val spark = SparkSession.builder().appName("SparkKinesis").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

val streamName = "mykinesisstream"
val endPointURL = "https://kinesis.ap-south-1.amazonaws.com"
val regionName = "ap-south-1"
val AWS_ACCESS_KEY_ID = ""
val SECRET_ACCESS_ID = ""
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
  
 // var wc = WordCount("Word",10)
val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count().as[WordCount]
 // val query = wordCounts.writeStream.outputMode("complete").foreachBatch(new DynamoDbWriter[WordCount]{}).start()
 
 
  val query = wordCounts.writeStream.outputMode("complete").foreachBatch { (batchDF: Dataset[WordCount], batchId: Long) =>

    batchDF.write.format("dynamodb").option("tableName","table1").save()      // Use Cassandra batch data source to write streaming out
      
  }.start()
   query.awaitTermination()     
     
        
}
