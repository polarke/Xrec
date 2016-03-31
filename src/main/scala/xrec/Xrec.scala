package xrec

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

abstract class Xrec[User: ClassTag, Item: ClassTag] {
  
//  implicit val userTag: ClassTag[User]
//  implicit val itemTag: ClassTag[Item]
  
  def loadUserRatings(sc: SparkContext, file: String,
                      parseLine: String => (User, Item, Double))
            : RDD[(User, Iterable[(Item, Double)])] =
    sc.textFile(file).map( line => {
      val (u, i, r) = parseLine(line)
      (u, (i, r))
    }).partitionBy(new XrecHashPartitioner[User, Item](numOfPartitions))
      .groupByKey()
      .persist()
  
  def loadItemRatings(sc: SparkContext, file: String,
                      parseLine: String => (User, Item, Double))
            : RDD[(Item, Iterable[(User, Double)])] =
    sc.textFile(file).map( line => {
      val (u, i, r) = parseLine(line)
      (i, (u, r))
    }).partitionBy(new XrecHashPartitioner[User, Item](numOfPartitions))
      .groupByKey()
      .persist()
  
  def loadTestRatings(sc: SparkContext, testFile: String,
                      parseLine: String => (User, Item, Double))
            : RDD[((User, Item), Double)] =
    sc.textFile(testFile).map( line => {
      val (u, i, r) = parseLine(line)
      ((u, i), r)
    }).partitionBy(new XrecHashPartitioner[User, Item](numOfPartitions))
      .persist()
    
  def mean(ratings: RDD[(User, Iterable[(Item, Double)])]): RDD[(User, Double)] =
    ratings.mapValues(x => {
      val sum = x.aggregate(0.0)((a, b) => a + b._2, _ + _)
      val n = x.size
      sum / n
    })
  
  def normalize(ratings: RDD[(Int, Iterable[(Int, Double)])]) = {
    val temp = ratings.mapValues { x =>
      val sum = x.aggregate(0.0)((a, b) => a + b._2, _ + _)
      val n = x.size
      val mean = sum / n
      (mean, x.map(y => (y._1, y._2 - mean)))
    }
    val means = temp.mapValues(_._1).persist()
    val normalizedRatings = temp.mapValues(_._2).persist()
    (means, normalizedRatings)
  }
  
  def predictByUserSimilarity(ratings: RDD[(User, Iterable[(Item, Double)])],
                              testcases: RDD[((User, Item), Double)],
                              similarities: RDD[(User, Iterable[(User, Double)])]):
            RDD[((User, Item), (Double, Double))] = {
    ratings.flatMapValues(iter => iter)
           .join(similarities.flatMapValues(iter => iter))
           .map { case (u, ((i,r), (v, s))) => ((v, i), (s*r, Math.abs(s))) } //
           .partitionBy(new XrecHashPartitioner[User, Item](numOfPartitions))
           .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
           .filter { case (_, (_, b)) => b != 0 } // replace this with better value in the future
           .mapValues { x => x._1 / x._2 }
           .join(testcases)
  }
  
  def predictByUserAverage(meansOfUsers: RDD[(User, Double)], testcases: RDD[((User, Item), Double)]):
            RDD[((User, Item), (Double, Double))] =
    testcases.mapPartitions(_.map{ case ((u, i), r) => (u, (i, r)) }, true)
      .join(meansOfUsers)
      .mapPartitions(_.map { case (u, ((i, r), m)) => ((u, i), (m, r)) }, true)
  
  def predictByItemAverage(meansOfItems: RDD[(Item, Double)], testcases: RDD[((User, Item), Double)]):
            RDD[((User, Item), (Double, Double))] = (
    testcases.map { case ((u, i), r) => (i, (u, r)) }
      .join(meansOfItems)
      .map { case (i, ((u, r), m)) => ((u, i), (m, r)) }
  )
  
  // if prediction for (a, b) is found in pred1, then use it. otherwise return the prediction of (a, b) in pred2
  def aggregate2Predictions(pred1: RDD[((User, Item), (Double, Double))], pred2: RDD[((User, Item), (Double, Double))]):
            RDD[((User, Item), (Double, Double))] = (
    pred1.rightOuterJoin(pred2)
         .mapValues {
           case (Some(a), b) => a
           case (None, b) => b
         }
  )
  
  def mae(pred: RDD[((User, Item), (Double, Double))]): Double = {
    val diffSum = pred.aggregate(0.0)(
        (a, b) => a + Math.abs(b._2._1 - b._2._2),
        _ + _
    )
    diffSum / pred.count()
  }
}

object XrecMain extends Xrec[Int, Int] {
  
  val numOfPartitions = 4
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val conf = new SparkConf().setMaster(s"local[8]").setAppName("xrec")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    
    val input = List((1, (2, 1.0)), (1, (3, 2.0)))
    val sim = sc.parallelize(input).groupByKey()
    val newsim = mean(sim)
    newsim.foreach { case (k, v) => println(k, v) }
    println(newsim)
  }
  
}

