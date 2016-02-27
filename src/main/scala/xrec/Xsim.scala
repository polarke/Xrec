package xrec

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

trait MultiplicationSimilarityExpander[T] {
  
  implicit val tTag: ClassTag[T]
  
  def expandSimilarity(rawSimilarities: RDD[(T, Iterable[(T, Double)])], iters: Int): RDD[(T, Iterable[(T, Double)])] = {
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
  
}

object ExpanderMain extends MultiplicationSimilarityExpander[Int] {
  
  implicit val tTag = ClassTag.Int
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val conf = new SparkConf().setMaster(s"local[8]").setAppName("xrec")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    
    val input = List((1, (2, 1.0)), (1, (3, 1.0)), (2, (1, 2.0)))
    val sim = sc.parallelize(input).groupByKey()
    val newsim = expandSimilarity(sim, 2)
    newsim.foreach { case (k, v) => println(k, v) }
  }
  
}