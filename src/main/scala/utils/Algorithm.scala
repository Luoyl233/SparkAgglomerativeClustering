package utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Algorithm extends Serializable{

  // Compute MST of Complete Graph
  def primLocal(points: Array[Point]): Array[Edge] = {
    val n = points.length
    val mst = new Array[Edge](n-1)
    val marked = Array.fill[Boolean](n)(false)
    val lowWeight = new Array[Double](n)  //Edge(labour, to, w), labour = labour(to), w = lowWeight(to)
    val neighbour = Array.fill[Int](n)(0)
    var minWeight = Double.MaxValue
    var minIndex = 0

    marked(0) = true
    for (i <- 1 until n) {
      lowWeight(i) = points(0).distanceTo(points(i))
    }

    // n-1 edges in mst
    for (k <- 0 until n-1) {
      // Find a edge with min weight
      for (i <- 0 until n ) {
        if (!marked(i) && lowWeight(i) < minWeight) {
          minWeight = lowWeight(i)
          minIndex = i
        }
      }

      // Add min edge to mst
      mst(k) = new Edge(points(neighbour(minIndex)).id, points(minIndex).id, minWeight)
      marked(minIndex) = true

      // Update lowWeight
      for (i <- 0 until n if !marked(i)) {
        val w = points(minIndex).distanceTo(points(i))
        if (w < lowWeight(i)) {
          neighbour(i) = minIndex
          lowWeight(i) = w
        }
      }
      minWeight = Double.MaxValue
    }

    mst.sortBy(_.weight)
  }

  def bipartiteMST(lPoints: Array[Point], rPoints: Array[Point]): Array[Edge] = {
    val m = lPoints.length
    val n = rPoints.length
    val total = m + n
    val allPoints = lPoints ++ rPoints
    val mst = new Array[Edge](total-1)
    val marked = Array.fill[Boolean](total)(false)
    val lowWeight = Array.fill[Double](total)(Double.MaxValue)
    val neighbour = Array.fill[Int](total)(0)
    var minWeight = Double.MaxValue
    var minIndex = 0

    //start from 0
    marked(0) = true
    for (i <- m until total){
      lowWeight(i) = allPoints(0).distanceTo(allPoints(i))
    }

    for (k <- 0 until total-1) {
      //find min edge
      for (i <- 0 until total) {
        if (!marked(i) && lowWeight(i) < minWeight) {
          minWeight = lowWeight(i)
          minIndex = i
        }
      }

      // Add min edge to mst
      val p = allPoints(minIndex)
      mst(k) = new Edge(allPoints(neighbour(minIndex)).id, p.id, minWeight)
      marked(minIndex) = true

      // Update lowWeight according to which side the point is on
      // If p on left side, then update the distance between p and the right side points
      if (minIndex < m) {
        for (i <- m until total if !marked(i)) {
          val w = p.distanceTo(allPoints(i))
          if (w < lowWeight(i)) {
            lowWeight(i) = w
            neighbour(i) = minIndex
          }
        }
      }
      else {
        for (i <- 0 until m if !marked(i)) {
          val w = p.distanceTo(allPoints(i))
          if (w < lowWeight(i)) {
            lowWeight(i) = w
            neighbour(i) = minIndex
          }
        }
      }
      minWeight = Double.MaxValue
    }

    mst.sortBy(_.weight)
  }


  def kruskalReducer(edges1: Array[Edge], edges2: Array[Edge]): Array[Edge] = {
    if (edges1 == null || edges1.isEmpty)
      return edges2
    if (edges2 == null || edges2.isEmpty)
      return edges1

    var mst = new ListBuffer[Edge]()
    val uf = new QuickUnionFind

    for (e <- edges1) {
      uf.add(e.from)
      uf.add(e.to)
    }
    for (e <- edges2) {
      uf.add(e.from)
      uf.add(e.to)
    }

    val n = uf.size
    val it1 = edges1.iterator.buffered
    val it2 = edges2.iterator.buffered
    var minEdge: Edge = null

    while (it1.hasNext && it2.hasNext && mst.length < n - 1) {
      minEdge =
        if (it1.head.weight <= it2.head.weight)
          it1.next()
        else
          it2.next()
      if (!uf.isConnected(minEdge.from, minEdge.to)) {
        mst += minEdge
        uf.union(minEdge.from, minEdge.to)
      }
    }

    while (it1.hasNext && mst.length < n - 1) {
      val e = it1.next()
      if (!uf.isConnected(e.from, e.to)) {
        mst += e
        uf.union(e.from, e.to)
      }
    }

    while (it2.hasNext && mst.length < n - 1) {
      val e = it2.next()
      if (!uf.isConnected(e.from, e.to)) {
        mst += e
        uf.union(e.from, e.to)
      }
    }

    mst.toArray
  }

  def kruskalReducer2(edges1: Array[Edge], edges2: Array[Edge]): Array[Edge] = {
    if (edges1 == null || edges1.isEmpty)
      return edges2
    if (edges2 == null || edges2.isEmpty)
      return edges1

    var mst = new ListBuffer[Edge]()
    val uf = new QuickUnionFind
    var i = 0
    var j = 0
    var minEdge: Edge = null

    for (e <- edges1) {
      uf.add(e.from)
      uf.add(e.to)
    }
    for (e <- edges2) {
      uf.add(e.from)
      uf.add(e.to)
    }

    val n = uf.size

    while (i < edges1.length && j < edges2.length && mst.length < n-1) {
      if (edges1(i).weight < edges2(j).weight) {
        minEdge = edges1(i)
        i += 1
      }
      else {
        minEdge = edges2(j)
        j += 1
      }
      if (!uf.isConnected(minEdge.from, minEdge.to)) {
        mst += minEdge
        uf.union(minEdge.from, minEdge.to)
      }
    }

    while (i < edges1.length && mst.length < n-1) {
      if (!uf.isConnected(edges1(i).from, edges1(i).to)) {
        mst += edges1(i)
        uf.union(edges1(i).from, edges1(i).to)
      }
      i += 1
    }
    while (j < edges2.length && mst.length < n-1) {
      if (!uf.isConnected(edges2(j).from, edges2(j).to)) {
        mst += edges2(j)
        uf.union(edges2(j).from, edges2(j).to)
      }
      j += 1
    }
    mst.toArray
  }

  def multiKruskalReducer(edgesIter: Iterator[Array[Edge]]): Array[Edge] = {
    val edgeArrays = edgesIter.toArray
    val numArr = edgeArrays.length

    if (numArr == 0)
      return null
    else if (numArr == 1)
      return edgeArrays(0)
    else if (numArr == 2)
      return kruskalReducer(edgeArrays(0), edgeArrays(1))

    val ord = Ordering.by[BufferedIterator[Edge], Double](_.head.weight).reverse
    val pq = new mutable.PriorityQueue[BufferedIterator[Edge]]()(ord)
    val uf = new QuickUnionFind
    val mst = new ListBuffer[Edge]()

    for (edges <- edgeArrays if edges != null && edges.nonEmpty) {
      for (e <- edges) {
        uf.add(e.from)
        uf.add(e.to)
      }
      // Use Buffered iterator to compare elements in pq without stepping next
      pq.enqueue(edges.iterator.buffered)
    }

    val n = uf.size

    while (pq.length > 1 && mst.size < n-1) {
      val minIter = pq.dequeue()
      val minEdge = minIter.next()

      if (!uf.isConnected(minEdge.from, minEdge.to)) {
        mst += minEdge
        uf.union(minEdge.from, minEdge.to)
      }

      if (minIter.hasNext)
        pq.enqueue(minIter)
    }

    // Only one iter left, merge it directly
    if (pq.length == 1) {
      val iter = pq.dequeue()
      while (iter.hasNext && mst.size < n - 1) {
        val edge = iter.next()
        if (!uf.isConnected(edge.from, edge.to)) {
          mst += edge
          uf.union(edge.from, edge.to)
        }
      }
    }

    mst.toArray
  }

}

