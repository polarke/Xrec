package xrec

import scala.reflect.ClassTag
import org.apache.spark.Partitioner

class XrecHashPartitioner[User: ClassTag, Item: ClassTag](numParts: Int) extends Partitioner {
  def numPartitions: Int = numParts
  
  def getPartition(key: Any): Int = key match {
    case (a: User, b) => {
      val mod = a.hashCode % numPartitions
      mod + (if (mod < 0) numPartitions else 0)
    }
    case a: User => {
      val mod = a.hashCode % numPartitions
      mod + (if (mod < 0) numPartitions else 0)
    }
    case a: Item => {
      val mod = a.hashCode % numPartitions
      mod + (if (mod < 0) numPartitions else 0)
    }
    case _ => throw new Exception("XrecHashPartitioner: invalid key!")
  }
  
  override def equals(other: Any): Boolean = other match {
    case h: XrecHashPartitioner[User, Item] =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
