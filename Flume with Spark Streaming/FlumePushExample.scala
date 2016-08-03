// Flume setup steps (and more) at http://spark.apache.org/docs/latest/streaming-flume-integration.html



import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.regex.Pattern
import java.util.regex.Matcher
import Utilities._
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/** Example of connecting to Flume in a "push" configuration. */
object FlumePushExample {
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "FlumePushExample", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a Flume stream receiving from a given host & port. It's that easy.
    val flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)
    
    // Except this creates a DStream of SparkFlumeEvent objects. We need to extract the actual messages.
    // This assumes they are just strings, like lines in a log file.
    // In addition to the body, a SparkFlumeEvent has a schema and header you can get as well. So you
    // could handle structured data if you want.
    val lines = flumeStream.map(x => new String(x.event.getBody().array()))
    
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

