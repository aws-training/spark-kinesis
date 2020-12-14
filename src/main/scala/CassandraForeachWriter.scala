
   import org.apache.spark.sql.ForeachWriter
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import model.WordCount
import org.apache.spark.rdd.RDD



   trait CassandraForeachWriter[RECORD] extends ForeachWriter[RECORD] {


val cassandraDriver = new CassandraDriver();
     override def open(partitionId: Long, version: Long): Boolean = {
       
       true
     }

    
     def process(record: WordCount) = {
    println(s"Process new $record")
    saveReading(record)
  }
  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    println(s"Close connection")
  }
    
def saveReading(record: WordCount)  = {
		cassandraDriver.connector.withSessionDo(session =>{
		  val query =s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink1} (word,count)
       values('${record.toTuple._1}',${record.toTuple._2})"""
      session.execute(query)
		}
    )
                              
	}

	
	
	
	


	

	



	
   }
