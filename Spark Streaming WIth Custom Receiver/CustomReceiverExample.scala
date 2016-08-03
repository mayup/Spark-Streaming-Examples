// Custom receiver docs at http://spark.apache.org/docs/latest/streaming-custom-receivers.html

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.io.{InputStreamReader, BufferedReader, InputStream}
import java.net.Socket
import org.apache.spark.streaming.receiver.Receiver

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

/** Example from the Spark documentation; this implements a socket
 *  receiver from scratch using a custom Receiver.
 */
class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
     // Connect to host:port
     socket = new Socket(host, port)

     // Until stopped or connection broken continue reading
     val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
     userInput = reader.readLine()
     while(!isStopped && userInput != null) {
       store(userInput)
       userInput = reader.readLine()
     }
     reader.close()
     socket.close()

     // Restart in an attempt to connect again when server is active again
     restart("Trying to connect again")
    } catch {
     case e: java.net.ConnectException =>
       // restart if could not connect to server
       restart("Error connecting to " + host + ":" + port, e)
     case t: Throwable =>
       // restart if there is any other error
       restart("Error receiving data", t)
    }
  }
}
 
/** Illustrates using a custom receiver to listen for Apache logs on port 7777
 *  and keep track of the top URL's in the past 5 minutes.
 */
object CustomReceiverExample {
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "CustomReceiverExample", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    val lines = ssc.receiverStream(new CustomReceiver("localhost", 7777))
    
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

