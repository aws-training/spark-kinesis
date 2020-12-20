import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import model.Flight
object SparkKinesisAirlineDataAnalysis extends App{
  val spark = SparkSession.builder().appName("SparkKinesis").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  
val streamName = "mykinesisstream"
val endPointURL = "https://kinesis.ap-south-1.amazonaws.com"
val regionName = "ap-south-1"
val AWS_ACCESS_KEY_ID = "AKIA43EB2YEDLHTRMUVM"
val SECRET_ACCESS_ID = "O9AX4R85Fc42WrarjEbXYgVCcyDkBcQmT9vGo52x"
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
  
  val flights = lines.map{line =>  assignFlightAttributes(line)}
      val dfWithUuid = flights.withColumn("route_id", expr("uuid()")).as[Flight]
  // Average arrival and departure delays in the year 2004 by month
  //val delays = flights.filter(_.year==2004).groupBy("year", "month").agg(avg("arrdelay"), avg("depdelay")).orderBy("year", "month")
    //  delays.writeStream.format("console").outputMode("complete").start()                    
  //val fQuery = flights.writeStream.format("console").outputMode("append").start()
  
  // No of flights flew every year
 /*val noOfFlights =  flights.groupBy("year").count()
 val flightsByYear = noOfFlights.select(noOfFlights("year"),noOfFlights("count").as("No of flights"))
               .writeStream
               .format("console")
               .outputMode("complete")
               .start()
   */            
 val query = dfWithUuid.writeStream.outputMode("append").foreachBatch { (batchDF: Dataset[Flight], batchId: Long) =>

    batchDF.write.format("dynamodb").option("tableName","flights").save()     
      
  }.start()
   query.awaitTermination()     
     
        
      def assignFlightAttributes(line: String)={
    val cols = line.split(",") 
    Flight("NA",
         cols(0).toInt,
         cols(1).toInt,
         cols(2).toInt,
         cols(3).toInt,
         cols(4).toInt,
         cols(5).toInt,
         cols(6).toInt,
         cols(7).toInt,
         cols(8),
         cols(9),
         cols(10),
         cols(11).toInt,
         cols(12).toInt,
         cols(13).toInt,
         cols(14).toInt,
         cols(15).toInt,
         cols(16),
         cols(17),
         cols(18).toInt,
         cols(19),
         cols(20), 
         cols(21),
         cols(22),
         cols(23),
         cols(24).toInt,
         cols(25).toInt,
         cols(26).toInt,
         cols(27).toInt,
         cols(28).toInt )
  }
        
}