

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

/** Illustrates using SparkSQL with Spark Streaming, to issue queries on 
 *  Apache log data extracted from a stream on port 9999.
 */
object LogSQL {
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogSQL", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the (URL, status, user agent) we want from each log line
    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })
 
    // Process each RDD from each batch as it comes in
    requests.foreachRDD((rdd, time) => {
      // So we'll demonstrate using SparkSQL in order to query each RDD
      // using SQL queries.
      
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // SparkSQL can automatically create DataFrames from Scala "case classes".
      // We created the Record case class for this purpose.
      // So we'll convert each RDD of tuple data into an RDD of "Record"
      // objects, which in turn we can convert to a DataFrame using toDF()
      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

      // Create a SQL table from this DataFrame
      requestsDataFrame.registerTempTable("requests")

      // Count up occurrences of each user agent in this RDD and print the results.
      // The powerful thing is that you can do any SQL you want here!
      // But remember it's only querying the data in this RDD, from this batch.
      val wordCountsDataFrame =
        sqlContext.sql("select agent, count(*) as total from requests group by agent")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
      
      // If you want to dump data into an external database instead, check out the
      // org.apache.spark.sql.DataFrameWriter class! It can write dataframes via
      // jdbc and many other formats! You can use the "append" save mode to keep
      // adding data from each batch.
    })
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

/** Case class for converting RDD to DataFrame */
case class Record(url: String, status: Int, agent: String)

/** Lazily instantiated singleton instance of SQLContext 
 *  (Straight from included examples in Spark)  */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}


