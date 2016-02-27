package xrec

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

abstract class CosineModel[User: ClassTag, Item: ClassTag] extends Xrec[User, Item] {
  
  def loadBaseRatings(sc: SparkContext, file: String, parseLine: String => (User, Item, Double)):
          (RDD[(User, Iterable[(Item, Double)])], RDD[(Item, Iterable[(User, Double)])]) = {
    val lines = sc.textFile(file)
    val ratingsOfUsers = lines.map( line => {
      val (u, i, r) = parseLine(line)
      (u, (i, r))
    } ).groupByKey().persist()
    val ratingsOfItems = ratingsOfUsers.flatMap { case (a, b) =>
      for (c <- b) yield (c._1, (a, c._2))
    }.groupByKey().persist()
    (ratingsOfUsers, ratingsOfItems)
  }
  
  def similarity(ratings: RDD[(Item, Iterable[(User, Double)])], k: Int): RDD[(User, Iterable[(User, Double)])] = {
    require(k >= 0)
    
    val allSimilarities = (
        ratings.flatMap(x => {
          val iter = x._2
          for (y1 <- iter; y2 <- iter if (y1._1 != y2._1))
            yield ((y1._1, y2._1), (y1._2 * y2._2, y1._2 * y1._2, y2._2 * y2._2))
        })
        reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) }
        map { case ((y1, y2), (a, b, c)) => (y1, (y2, a / Math.sqrt(b) / Math.sqrt(c))) }
        groupByKey()
    )
    if (k == 0) {
      allSimilarities.persist()
    } else {
      allSimilarities.mapValues(x =>
        x.foldLeft(List.empty[(User, Double)])((xs, y) =>
          if (xs.size < k) (y::xs).sortBy(e => e._2)
          else {
            val first = xs.head
            if (first._2 < y._2) y::(xs.tail).sortBy(e => e._2)
            else xs
          }
        ).reverse.toIterable).persist()
    }
  }
  
}

object CosineModel extends CosineModel[Int, Int] {
  
  implicit val userTag = ClassTag.Int
  implicit val itemTag = ClassTag.Int
  
  def numOfPartitions = 4
  
  val parseLine = (line: String) => {
    val arr = line.split("\t")
    (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val conf = new SparkConf().setMaster("local[8]").setAppName("xrec")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    val sc = new SparkContext(conf)
    
    val sourceFile = "src/main/resources/movielens/ml-100k/u1.base"
    val testFile = "src/main/resources/movielens/ml-100k/u1.test"

    val (ratingsOfUsers, ratingsOfItems) = loadBaseRatings(sc, sourceFile, parseLine)
    val similaritiesOfUsers = similarity(ratingsOfItems, 0)
    val meansOfUsers = mean(ratingsOfUsers)
    
    val testRatings = loadTestRatings(sc, testFile, parseLine)
    val pred1 = predictByUserSimilarity(ratingsOfUsers, testRatings, similaritiesOfUsers)
    val pred2 = predictByUserAverage(meansOfUsers, testRatings)
    
    val pred = aggregate2Predictions(pred1, pred2)
    System.err.println(mae(pred2), mae(pred))
  }
}