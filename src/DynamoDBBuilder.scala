import com.amazonaws.AmazonServiceException;
 import com.amazonaws.auth._;
 import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
 import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
 import com.amazonaws.services.dynamodbv2.model.AttributeValue;
 import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
 import com.amazonaws.client.builder.AwsClientBuilder

class DynamoDBBuilder extends Serializable {
  // Build a spark session. Class is made serializable so to get access to SparkSession in a driver and executors. 
  // Note here the usage of @transient lazy val 
  def buildDynamoDbConnection: AmazonDynamoDB =  {
    val endPointURL = new AwsClientBuilder.EndpointConfiguration("https://dy.namodb.ap-south-1.amazonaws.com","ap-south-1")

    val accessKey = "AKIA43EB2YEDLHTRMUVM"
     val secretKey = "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"
     val regionName = "ap-south-1"
     val ddb = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
      .withEndpointConfiguration(endPointURL)
      .build()
      ddb
  }
}
