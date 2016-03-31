package xrec

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

abstract class PearsonModel[User: ClassTag, Item: ClassTag] extends Xrec[User, Item] {
  
  def loadBaseRatings(sc: SparkContext, file: String, parseLine: String => (User, Item, Double)):
           (RDD[(User, Double)],
            RDD[(User, Iterable[(Item, Double)])],
            RDD[(Item, Iterable[(User, Double)])]) = {
    val lines = sc.textFile(file)
    val ratings = lines.map(line => { val (u, i, r) = parseLine(line); (u, (i, r)) })
                       .groupByKey(new XrecHashPartitioner[User, Item](numOfPartitions))
    normalize(ratings)
  }
  
  def normalize(ratingsOfUsers: RDD[(User, Iterable[(Item, Double)])]):
           (RDD[(User, Double)],
            RDD[(User, Iterable[(Item, Double)])],
            RDD[(Item, Iterable[(User, Double)])]) = {
    val temp = ratingsOfUsers.mapValues { x =>
      val sum = x.aggregate(0.0)((a, b) => a + b._2, _ + _)
      val n = x.size
      val mean = sum / n
      (mean, x.map(y => (y._1, y._2 - mean)))
    }
    val meansOfUsers = temp.mapValues(_._1).persist()
    val normalizedRatingsOfUsers = temp.mapValues(_._2).persist()
    val normalizedRatingsOfItems = normalizedRatingsOfUsers.flatMap { case (a, b) =>
      for (c <- b) yield (c._1, (a, c._2))
    }.groupByKey(new XrecHashPartitioner[User, Item](numOfPartitions))
     .persist()
    (meansOfUsers, normalizedRatingsOfUsers, normalizedRatingsOfItems)
  }
  
  def similarity(normalizedRatings: RDD[(Item, Iterable[(User, Double)])], k: Int):
          RDD[(User, Iterable[(User, Double)])] = {
    require(k >= 0)
    
    val allSimilarities = 
      normalizedRatings.flatMap(x => {
        val iter = x._2
        for (y1 <- iter; y2 <- iter if (y1._1 != y2._1))
          yield ((y1._1, y2._1), (y1._2 * y2._2, y1._2 * y1._2, y2._2 * y2._2))
      }).partitionBy(new XrecHashPartitioner[User, Item](numOfPartitions))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
        .filter { case (_, (_, b, c)) => (b != 0) && (c != 0) }
        .mapPartitions(_.map { case ((y1, y2), (a, b, c)) => (y1, (y2, a / Math.sqrt(b) / Math.sqrt(c))) }, true)
        .groupByKey()
    if (k == 0) {
      allSimilarities
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
      )
    }
  }
  
  def predict(normalizedRatings: RDD[(User, Iterable[(Item, Double)])],
                         means: RDD[(User, Double)],
                         testcases: RDD[((User, Item), Double)],
                         similarities: RDD[(User, Iterable[(User, Double)])]):
            RDD[((User, Item), (Double, Double))] =
    predictByUserSimilarity(normalizedRatings, testcases, similarities)
      .mapPartitions(_.map { case ((u, i), (a, b)) => (u, (i, (a, b))) }, true)
      .join(means)
      .mapPartitions(_.map { case (u, ((i, (a, b)), m)) => {
          var x = (a + m + 0.5).toInt.toDouble
          if (x > 5.0) x = 5.0
          else if (x < 1.0) x = 1.0
          ((u, i), (x, b))
        } 
      }, true)
      
  def run(sc: SparkContext, sourceFile: String, testFile: String, parseLine: String => (User, Item, Double), K: Int) = {
    val (meansOfUsers, normalizedRatingsOfUsers, normalizedRatingsOfItems) =
      loadBaseRatings(sc, sourceFile, parseLine)
    val similaritiesOfUsers = similarity(normalizedRatingsOfItems, 0)
    
    val testRatings = loadTestRatings(sc, testFile, parseLine)
    val pred1 = predict(normalizedRatingsOfUsers, meansOfUsers, testRatings, similaritiesOfUsers).persist()
    val pred2 = predictByUserAverage(meansOfUsers, testRatings).persist()
    
    val pred = aggregate2Predictions(pred1, pred2).persist()
  }
  
  def run(sc: SparkContext, sourceRatings: RDD[(User, Iterable[(Item, Double)])], testRatings: RDD[((User, Item), Double)], K: Int) = {
    val (meansOfUsers, normalizedRatingsOfUsers, normalizedRatingsOfItems) = normalize(sourceRatings)
    val similaritiesOfUsers = similarity(normalizedRatingsOfItems, 0)
    
    val pred1 = predict(normalizedRatingsOfUsers, meansOfUsers, testRatings, similaritiesOfUsers).persist()
    val pred2 = predictByUserAverage(meansOfUsers, testRatings).persist()
    
    val pred = aggregate2Predictions(pred1, pred2).persist()
  }

}

object PearsonModelMain extends PearsonModel[Int, Int] with MultiplicationSimilarityExpander[Int] {
  
  override val tTag = ClassTag.Int
  
  val parseLine = (line: String) => {
    val arr = line.split("\t")
    (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val conf = new SparkConf().setMaster(s"local[8]").setAppName("xrec")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val sourceFile = "src/main/resources/movielens/ml-100k/u1.base"
    val testFile = "src/main/resources/movielens/ml-100k/u1.test"

    val (meansOfUsers, normalizedRatingsOfUsers, normalizedRatingsOfItems) =
      loadBaseRatings(sc, sourceFile, parseLine)
    val similaritiesOfUsers = similarity(normalizedRatingsOfItems, 0)
    
    val testRatings = loadTestRatings(sc, testFile, parseLine)
    val pred1 = predict(normalizedRatingsOfUsers, meansOfUsers, testRatings, similaritiesOfUsers).persist()
    val pred2 = predictByUserAverage(meansOfUsers, testRatings).persist()
    
    val pred = aggregate2Predictions(pred1, pred2).persist()
    println(mae(pred2), mae(pred))
  }
}