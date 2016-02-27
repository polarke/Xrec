package xrec

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.scalatest._

import PearsonModelMain._

class XrecSuite extends FunSuite {
  
  val parseLine = (line: String) => {
    val arr = line.split("\t")
    (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
  }
  
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setMaster("local[4]").setAppName("xrec")
  val sc = new SparkContext(conf)
  
  val (meansOfUsers, normalizedRatingsOfUsers, normalizedRatingsOfItems) = loadBaseRatings(sc, "src/main/resources/dummy", parseLine)
  
  test("should compute the means of users") {
    assert(meansOfUsers.map {
      case (1, avg) => avg == 2.0
      case (2, avg) => avg == 7.0 / 3
      case (3, avg) => avg == 10.0 / 3
    } reduce (_ && _))
  }
  
  test("should compute the normalized ratings of users") {
    assert(
      normalizedRatingsOfUsers.flatMap {
        case (a, b) =>
          for (c <- b) yield ((a, c._1), c._2)
      } map {
        case ((1, 4), rating) => rating == -1.0
        case ((1, 5), rating) => rating == 1.0
        case ((1, 6), rating) => rating == 0.0
        case ((2, 4), rating) => rating == 3 - 7.0 / 3
        case ((2, 5), rating) => rating == 0 - 7.0 / 3
        case ((2, 6), rating) => rating == 4 - 7.0 / 3
        case ((3, 4), rating) => rating == 2.0 - 10.0 / 3
        case ((3, 5), rating) => rating == 4.0 - 10.0 / 3
        case ((3, 6), rating) => rating == 4.0 - 10.0 / 3
        case _ => true
      } reduce (_ && _)
    )
  }
  
  test("should compute the normalized ratings of items") {
    assert(
      normalizedRatingsOfItems.flatMap {
        case (a, b) =>
          for (c <- b) yield ((c._1, a), c._2)
      } map {
        case ((1, 4), rating) => rating == -1.0
        case ((1, 5), rating) => rating == 1.0
        case ((1, 6), rating) => rating == 0.0
        case ((2, 4), rating) => rating == 3 - 7.0 / 3
        case ((2, 5), rating) => rating == 0 - 7.0 / 3
        case ((2, 6), rating) => rating == 4 - 7.0 / 3
        case ((3, 4), rating) => rating == 2.0 - 10.0 / 3
        case ((3, 5), rating) => rating == 4.0 - 10.0 / 3
        case ((3, 6), rating) => rating == 4.0 - 10.0 / 3
        case _ => true
      } reduce (_ && _)
    )
  }
  
  val simOfUser = similarity(normalizedRatingsOfItems, 0)
  
  test("should compute the pearson user similarities") {
    assert(
      simOfUser.flatMap {
        case (a, b) =>
          for (c <- b) yield ((a, c._1), c._2)
      } map {
        case ((4, 5), s) => s == -1.5 / Math.sqrt(39)
        case ((4, 6), s) => s == Math.sqrt(3) / 2.0
        case ((5, 4), s) => s == -4.0 / Math.sqrt(39)
//        case ((2, 3), s) => s == 0
//        case ((3, 1), s) => s == 0
//        case ((3, 2), s) => s == 0
        case _ => true
      } reduce (_ && _)
    )
  }
   
  val simOfItem = similarity(normalizedRatingsOfUsers, 0)
  
  test("should compute the pearson item similarities") {
    assert(
      simOfItem.flatMap {
        case (a, b) =>
          for (c <- b) yield ((a, c._1), c._2)
      } map {
        case ((4, 5), s) => s == -4.0 / Math.sqrt(39)
        case ((4, 6), s) => s == Math.sqrt(3) / 2.0
        case ((5, 4), s) => s == -4.0 / Math.sqrt(39)
//        case ((2, 3), s) => s == 0
//        case ((3, 1), s) => s == 0
//        case ((3, 2), s) => s == 0
        case _ => true
      } reduce (_ && _)
    )
  }
  
  val testcases = loadTestRatings(sc, "src/main/resources/dummy", parseLine)
  val predByUserAverage = predictByUserAverage(meansOfUsers, testcases)
  
  test("should predict by user average") {
    assert(predByUserAverage.map {
      case ((1, _), (avg, _)) => avg == 2.0
      case ((2, _), (avg, _)) => avg == 7.0 / 3
      case ((3, _), (avg, _)) => avg == 10.0 / 3
    } reduce (_ && _))
  }
  
//  val predByItemAverage = predictByItemAverage(meansOfItems, testcases)
//  test("should predict by item average") {
//    assert(predByItemAverage.map {
//      case ((_, 4), (avg, _)) => avg == 2.0
//      case ((_, 5), (avg, _)) => avg == 7.0 / 3
//      case ((_, 6), (avg, _)) => avg == 10.0 / 3
//    } reduce (_ && _))
//  }
  
}