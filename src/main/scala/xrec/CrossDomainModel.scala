package xrec

import scala.reflect.classTag
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

abstract class CrossDomainModel[User: ClassTag, Item: ClassTag] extends PearsonModel[User, Item] {
  
  def parseLine: String => (User, Item, Double)
  def isSourceUser: User => Boolean
  def isTargetUser: User => Boolean
  
  def expandSimilarity(rawSimilarities: RDD[(User, Iterable[(User, Double)])], iters: Int): RDD[(User, Iterable[(User, Double)])] = {
    require(rawSimilarities.partitioner.isDefined)
    val partitioner = rawSimilarities.partitioner.get
    
    var expandedSimilarities = rawSimilarities
    for (i <- 1 to iters) {
      val length2Similarities = expandedSimilarities.flatMap { case (_, iterable) =>
        for (y1 <- iterable; y2 <- iterable if (y1._1 != y2._1))
          yield (y1._1, (y2._1, y1._2 * y2._2))
      }.groupByKey(partitioner)
      //expandedSimilarities = mergeSimilarities(expandedSimilarities, length2Similarities)
      // to do : check top-k
      val joint = expandedSimilarities.cogroup(length2Similarities)
      expandedSimilarities = joint.mapValues { case (a, b) =>
        if (b.size == 0) {
          a.head
        } else if (a.size == 0) {
          b.head
        } else {
          var ma = scala.collection.mutable.Map() ++ a.head
          val mb = b.head.toMap
          for (x <- mb) {
            val k = x._1
            val vb = x._2
            val va = ma.getOrElse(k, Double.MinValue)
            ma.update(k, Math.max(va, vb))
          }
          ma
        }
      }
    }
    expandedSimilarities
  }
  
  def run(sc: SparkContext, bothDomain: String, sourceDomain: String, targetDomain: String, testCases: String) = {
    val sourceRatings = loadUserRatings(sc, sourceDomain, parseLine)
    val targetRatings = loadUserRatings(sc, targetDomain, parseLine)
    val (meansOfUsers, normalizedRatingsOfUsers, normalizedRatingsOfItems) = normalize(sourceRatings ++ targetRatings)
    val rawSimilaritiesOfUsers = similarity(normalizedRatingsOfItems, 0)
    val similaritiesOfUsers = expandSimilarity(rawSimilaritiesOfUsers, 1)
    val crossSimilarities = similaritiesOfUsers.filter(x => isSourceUser(x._1))
                                               .mapValues(y => y.filter(z => isTargetUser(z._1)))
                                               .mapValues(y => y.maxBy(_._2))
    val mappedRatings = sourceRatings.join(crossSimilarities)
                                     .flatMap{case (a, (b, c)) =>
                                       for (d <- b) yield ((c._1, d._1), d._2)
                                     }
    val targetProfile = targetRatings.flatMap{case (u, iter) => for (x <- iter) yield ((u, x._1), x._2)}
                 .cogroup(mappedRatings)
                 .mapValues{case (a, b) =>
                   if (a.size == 0) {
                     b.max
                   } else {
                     a.head
                   }
                 }
                 .map{case ((u, i), r) => (i, (u, r))}
                 .groupByKey(new XrecHashPartitioner[User, Item](numOfPartitions))
    object itemPearsonModel extends PearsonModel[Item, User]
    val testRatings = itemPearsonModel.loadTestRatings(sc, testCases, line => {val (u, i, r) = parseLine(line); (i, u, r)})
    itemPearsonModel.run(sc, targetProfile, testRatings, 0)
  }
}

object CrossDomainModelMain extends CrossDomainModel[Int, Int] {
  
  val userTag = ClassTag.Int
  val itemTag = ClassTag.Int
  def isTargetUser = user => user >= 1000
  def isSourceUser = user => user <= 1000
  
  def numOfPartitions = 80
  
  val parseLine = (line: String) => {
    val arr = line.split("\t")
    (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
  }

}