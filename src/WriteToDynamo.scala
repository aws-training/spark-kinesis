import org.apache.spark.sql.SparkSession

object WriteToDynamo extends App{
  val spark = SparkSession.builder().appName("Dynamo")
  .config("dynamodb.servicename", "dynamodb")
//.config("dynamodb.input.tableName", "myDynamoDBTable")   // Pointing to DynamoDB table
.config("dynamodb.endpoint", "dynamodb.ap-south-1.amazonaws.com")
.config("dynamodb.regionid", "ap-south-1")
.config("dynamodb.throughput.read", "1")
.config("dynamodb.throughput.read.percent", "1")
.config("dynamodb.version", "2011-12-05").master("local").getOrCreate()


val df  = spark.read.csv("a.csv")
df.write.format("dynamodb").save()

}