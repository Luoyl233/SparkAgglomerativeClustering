package utils

class Edge(val from: Long, val to: Long, val weight: Double) extends Serializable {

  override def toString: String = {
    "(%d, %d, %g)".format(from, to, weight)
  }
}
