

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD

import Utilities._

/** Example of using streaming linear regression with stochastic gradient descent.
 *  Listens to port 9999 for data on page speed vs. amount spent, and creates
 *  a linear model to predict amount spent by page speed over time.
 */
object StreamingRegression {
  
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "StreamingRegression", Seconds(1))
    
    setupLogging()
    
    // Create a socket stream to listen for training data on port 9999
    // This will listen for x,y number pairs where x is the "label" we want to predict
    // and y is the "feature" which is some associated value we can use to predict the label by
    // Note that currently MLLib only works properly if this input data is first scaled such that
    // it ranges from -1 to 1 and has a mean of 0, more or less! You need to scale it down, then
    // remember to scale it back up later.
    val trainingLines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // And another stream that listens for test data on port 7777
    // This will expect both the known label and the feature data. In the real world you won't know
    // the "correct" value and would just input feature data, but this lets us validate our results.
    val testingLines = ssc.socketTextStream("127.0.0.1", 7777, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Convert input data to LabeledPoints for MLLib
    val trainingData = trainingLines.map(LabeledPoint.parse).cache()
    val testData = testingLines.map(LabeledPoint.parse)
    
    // Just so we see something happen when training data is received
    trainingData.print()
    
    // Now we will build up our linear regression model as new training data is received.
    val numFeatures = 1
    val model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures))
    model.algorithm.setIntercept(true) // Needed to allow the model to have a non-zero Y intercept
    
    model.trainOn(trainingData)
    
    // And as test data is received, we'll run our model on it to predict values based on our linear
    // regression. By using predictOnValues we can carry along our "known correct" labels data. In
    // the real world you wouldn't have this and would just call predictOn() instead which only expects
    // feature data.
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
  
}