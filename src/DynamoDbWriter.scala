import org.apache.spark.sql.{ForeachWriter, Row}
 import com.amazonaws.AmazonServiceException;
 import com.amazonaws.auth._;
 import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
 import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
 import com.amazonaws.services.dynamodbv2.model.AttributeValue;
 import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
 import java.util.ArrayList;
 import model.WordCount
 import scala.collection.JavaConverters._
 import com.amazonaws.client.builder.AwsClientBuilder

 trait DynamoDbWriter[RECORD] extends ForeachWriter[RECORD] {
   /* private val tableName = "value"
    private val accessKey = "AKIA43EB2YEDLHTRMUVM"
    private val secretKey = "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"
    private val regionName = "ap-south-1"
val endPointURL = new AwsClientBuilder.EndpointConfiguration("https://dy.namodb.ap-south-1.amazonaws.com","ap-south-1")
    // This will lazily be initialized only when open() is called
    lazy val ddb = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
      .withRegion(regionName).withEndpointConfiguration(endPointURL)
      .build()
*/
   val ddb =  new DynamoDBBuilder().buildDynamoDbConnection
    //
    // This is called first when preparing to send multiple rows.
    // Put all the initialization code inside open() so that a fresh
    // copy of this class is initialized in the executor where open()
    // will be called.
    //
    def open(partitionId: Long, epochId: Long) = {
      //ddb  // force the initialization of the client
      true
    }

    //
    // This is called for each row after open() has been called.
    // This implementation sends one row at a time.
    // A more efficient implementation can be to send batches of rows at a time.
    //
     def process(row: WordCount) = {
      val rowAsMap =  Map("Key"->"Value")
      val dynamoItem = rowAsMap.mapValues {
        v: Any => new AttributeValue(v.toString)
      }.asJava

     ddb.putItem("tableName", dynamoItem)
  }

  //
  // This is called after all the rows have been processed.
  //
  def close(errorOrNull: Throwable) = {
    ddb.shutdown()
  }
}