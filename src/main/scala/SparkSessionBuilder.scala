import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

class SparkSessionBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors. 
  // Note here the usage of @transient lazy val 
  def buildCassandraSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
    .setAppName("Structured Streaming from Kafka to Cassandra")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")
    @transient lazy val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
    spark
  }
}
