
   import org.apache.spark.sql.ForeachWriter
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import model.Flight
import org.apache.spark.rdd.RDD



   trait CassandraForeachWriter[RECORD] extends ForeachWriter[RECORD] {


val cassandraDriver = new CassandraDriver();
     override def open(partitionId: Long, version: Long): Boolean = {
       
       true
     }

    
     def process(record: Flight) = {
    println(s"Process new $record")
    saveRecord(record)
  }
  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    //println(s"Close connection")
  }
    
def saveRecord(record: Flight)  = {
		cassandraDriver.connector.withSessionDo(session =>{
		  val query =s"""
       insert into ${cassandraDriver.namespace}.${cassandraDriver.foreachTableSink1} (key , year ,month ,dayofmonth ,dayofweek ,
  deptime , crsdeptime , arrtime , crsarrtime , uniquecarrier ,
  flightnum , tailnum , actualelapsedtime ,crselapsedtime , airtime ,
  arrdelay , depdelay , origin , dest , distance , taxiin ,
  taxiout ,cancelled , cancellationcode , diverted , carrierdelay ,
  weatherdelay , nasdelay , securitydelay , lateaircraftdelay) 
       values(now(),
            ${record.year},
            ${record.month},
            ${record.dayofmonth},
            ${record.dayofweek},
            ${record.deptime},
            ${record.crsdeptime},
            ${record.arrtime},
            ${record.crsarrtime},
            '${record.uniquecarrier}',
            '${record.flightnum}',
            '${record.tailnum}',
            ${record.actualelapsedtime},
            ${record.crselapsedtime},
            ${record.airtime},
            ${record.arrdelay},
            ${record.depdelay},
           '${record.origin}',
            '${record.dest}',
            ${record.distance},
            '${record.taxiin}',
            '${record.taxiout}',
            '${record.cancelled}',
            '${record.cancellationcode}',
            '${record.diverted}',
            ${record.carrierdelay},
            ${record.weatherdelay},
            ${record.nasdelay},
            ${record.securitydelay},
            ${record.lateaircraftdelay}
      

            )"""
      session.execute(query)
		}
    )
                              
	}

	
	
	
	


	

	



	
   }
