// Kinesis setup steps (and more) at http://spark.apache.org/docs/latest/streaming-kinesis-integration.html


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

 import org.apache.spark.streaming.Duration
 import org.apache.spark.streaming.kinesis._
 import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
 
/** Example of connecting to Amazon Kinesis Streaming and listening for log data. */
object KinesisExample {
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "KinesisExample", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a Kinesis stream. You must create an app name unique for this region, and specify
    // stream name, Kinesis endpoint, and region you want. 
    val kinesisStream = KinesisUtils.createStream(
     ssc, "Unique App Name", "Stream Name", "kinesis.us-east-1.amazonaws.com",
     "us-east-1", InitialPositionInStream.LATEST, Duration(2000), StorageLevel.MEMORY_AND_DISK_2)
     
    // This gives you a byte array for each message. Let's assume these represent strings.
    val lines = kinesisStream.map(x => new String(x))
    
    // Extract the request field from each log line
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    
    // Extract the URL from the request
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    
    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

