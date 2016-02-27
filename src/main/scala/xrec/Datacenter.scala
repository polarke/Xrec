package xrec

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.Logging
import org.apache.log4j.Logger
import org.apache.log4j.Level

// change parent class to run on different similarity metric
object Datacenter extends PearsonModel[Int, Int] {
  
  implicit val userTag = ClassTag.Int
  implicit val itemTag = ClassTag.Int
  
  val sourceFile = "/source/file"
  val testFile = "/test/file"
  
  // change here for item model
  val parseLine = (line: String) => {
    val arr = line.split("\t")
    (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val conf = new SparkConf().setAppName("xrec")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val (meansOfUsers, normalizedRatingsOfUsers, normalizedRatingsOfItems) = loadBaseRatings(sc, sourceFile, parseLine)
    val similaritiesOfUsers = similarity(normalizedRatingsOfItems, 0).persist()
    
    val testRatings = loadTestRatings(sc, testFile, parseLine)
    val pred1 = predict(normalizedRatingsOfUsers, meansOfUsers, testRatings, similaritiesOfUsers).persist()
    val pred2 = predictByUserAverage(meansOfUsers, testRatings).persist()
    
    val pred = aggregate2Predictions(pred1, pred2).persist()

    val mae1 = mae(pred2)
    val mae2 = mae(pred)
    println(s"=====> userAvgs: $mae1, simModel: $mae2")
  }
}