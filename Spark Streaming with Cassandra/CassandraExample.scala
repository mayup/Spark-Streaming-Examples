

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import com.datastax.spark.connector._

/** Listens to Apache log data on port 9999 and saves URL, status, and user agent
 *  by IP address in a Cassandra database.
 */
object CassandraExample {
  
  def main(args: Array[String]) {

    // Set up the Cassandra host address
    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "127.0.0.1")
    conf.setMaster("local[*]")
    conf.setAppName("CassandraExample")
    
    // Create the context with a 10 second batch size
    val ssc = new StreamingContext(conf, Seconds(10))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the (IP, URL, status, useragent) tuples that match our schema in Cassandra
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = scala.util.Try(requestFields(1)) getOrElse "[error]"
        (ip, url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", "error", 0, "error")
      }
    })
    
    // Now store it in Cassandra
    requests.foreachRDD((rdd, time) => {
      rdd.cache()
      println("Writing " + rdd.count() + " rows to Cassandra")
      rdd.saveToCassandra("frank", "LogTest", SomeColumns("IP", "URL", "Status", "UserAgent"))
    })
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

