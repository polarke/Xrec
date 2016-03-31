package xrec

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level


abstract class AdjustedCosineModel[User: ClassTag, Item: ClassTag] extends Xrec[User, Item] {

  def loadBaseRatings(sc: SparkContext, file: String, parseLine: String => (User, Item, Double)):
           (RDD[(Item, Double)],
            RDD[(User, Iterable[(Item, Double)])],
            RDD[(Item, Iterable[(User, Double)])]) = {
    val lines = sc.textFile(file)
    val ratingsOfItems = lines.map( line => {
      val (u, i, r) = parseLine(line)
      (i, (u, r))
    }).groupByKey().persist()
    val temp = ratingsOfItems.mapValues { x =>
      val sum = x.aggregate(0.0)((a, b) => a + b._2, _ + _)
      val n = x.size
      val mean = sum / n
      (mean, x.map(y => (y._1, y._2 - mean)))
    }.persist()
    val meansOfItems = temp.mapValues(_._1).persist()
    val normalizedRatingsOfItems= temp.mapValues(_._2).persist()
    val normalizedRatingsOfUsers = normalizedRatingsOfItems.flatMap { case (a, b) =>
      for (c <- b) yield (c._1, (a, c._2))
    }.groupByKey().persist()
    (meansOfItems, normalizedRatingsOfUsers, normalizedRatingsOfItems)
  }
  
  def similarity(normalizedRatingsOfItems: RDD[(Item, Iterable[(User, Double)])], k: Int):
          RDD[(User, Iterable[(User, Double)])] = {
    require(k >= 0)
    
    val allSimilarities = (
        normalizedRatingsOfItems.flatMap(x => {
          val iter = x._2
          for (y1 <- iter; y2 <- iter if (y1._1 != y2._1))
            yield ((y1._1, y2._1), (y1._2 * y2._2, y1._2 * y1._2, y2._2 * y2._2))
        })
        reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) }
        filter { case (_, (_, b, c)) => (b != 0) && (c != 0) }
        map { case ((y1, y2), (a, b, c)) => (y1, (y2, a / Math.sqrt(b) / Math.sqrt(c))) }
        groupByKey()
    )
    if (k == 0) {
      allSimilarities.persist()
    } else {
      allSimilarities.mapValues(x => 
        // take top-k elements
        x.foldLeft(List.empty[(User, Double)])((xs, y) =>
          if (xs.size < k) (y::xs).sortBy(e => e._2)
          else {
            val first = xs.head
            if (first._2 < y._2) y::(xs.tail).sortBy(e => e._2)
            else xs
          }
        ).reverse.toIterable
      ).persist()
    }
  }
  
  def predict(normalizedRatings: RDD[(User, Iterable[(Item, Double)])],
                                                   means: RDD[(Item, Double)],
                                                   testcases: RDD[((User, Item), Double)],
                                                   similarities: RDD[(User, Iterable[(User, Double)])]):
            RDD[((User, Item), (Double, Double))] = (
    predictByUserSimilarity(normalizedRatings, testcases, similarities)
    map { case ((u, i), (a, b)) => (i, (u, (a, b))) }
    join means
    map { case (i, ((u, (a, b)), m)) => {
        var x = (a + m + 0.5).toInt.toDouble
        if (x > 5.0) x = 5.0
        else if (x < 1.0) x = 1.0
        ((u, i), (x, b))
      } 
    } persist()
  )
  
}

object AdjustedCosineModel extends AdjustedCosineModel[Int, Int] {
  
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

    val (meansOfItems, normalizedRatingsOfUsers, normalizedRatingsOfItems) =
      loadBaseRatings(sc, sourceFile, parseLine)
    val similaritiesOfUsers = similarity(normalizedRatingsOfItems, 0)
    
    val testRatings = loadTestRatings(sc, testFile, parseLine)
    val pred1 = predict(normalizedRatingsOfUsers, meansOfItems, testRatings, similaritiesOfUsers)
    val pred2 = predictByItemAverage(meansOfItems, testRatings)
    
    val pred = aggregate2Predictions(pred1, pred2)
    System.err.println(mae(pred2), mae(pred))
  }
}