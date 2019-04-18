package utils

import scala.collection.mutable

class QuickUnionFind
  extends Serializable {

  private val parent = new mutable.HashMap[Long, Long]()

  def size: Int = parent.size

  def add(x: Long): Unit = {
    if (!parent.contains(x))
      parent += (x -> -1L)
  }

  def +=(x: Long): Unit = add(x)

  // Find with path compress
  def find(x: Long): Long = {
    var p = parent(x)
    if (p < 0) {
      x
    }
    else {
      p = find(p)
      parent(x) = p
      p
    }
  }

  // Weighted union
  def union(x: Long, y: Long): Unit = {
    val p1 = find(x)
    val p2 = find(y)
    val n1 = parent(p1)
    val n2 = parent(p2)
    if (n2 < n1) {
      parent(p1) = p2
      parent(p2) = n1 + n2
    }
    else {
      parent(p2) = p1
      parent(p1) = n1 + n2
    }
  }

  def isConnected(x: Long, y: Long): Boolean = find(x) == find(y)

  def pairs(): Iterable[(Long, Long)] = {
    parent.keys.map(id => (id, find(id)))
  }
}
