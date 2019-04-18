package shas

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils._

import scala.collection.mutable.ArrayBuffer


class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.toString
}


class HierarchicalClustering(val s: Int,
                             val k: Int,
                             val outputFilePath: String,
                             val nClusters: Int = 2)
  extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger("SHAS")
  logger.setLevel(Level.ALL)

  val slicesPath: String =  outputFilePath + "slices/"

  def createPartition(n: Int, numPartition: Int, sc: SparkContext): RDD[(Int, Int)] = {
    sc.parallelize(0 until n).flatMap {x =>
      for (i <- x until n) yield (x, i)
    }.repartition(numPartition)
  }

  def readPointsFromHDFS(path: String): Array[Point] = {
    val hdfsConf = new Configuration()
    val HADOOP_HOME = sys.env("HADOOP_HOME")
    hdfsConf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"))
    hdfsConf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"))
    var points = new ArrayBuffer[Point]()

    val fs = FileSystem.get(hdfsConf)
    val br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
    var line = br.readLine()
    while (line != null) {
      val pairs = line.split("#")
      val coords = pairs(1).substring(1, pairs(1).length-1).split(",").map(_.toDouble)
      points += new Point(pairs(0).toLong, coords)
      line = br.readLine()
    }
    points.toArray
  }

  def localMST(ids: (Int, Int)): Array[Edge] = {
    val lid = ids._1
    val rid = ids._2

    if (lid == rid) {
      val points = readPointsFromHDFS(slicesPath + lid)
      Algorithm.primLocal(points)
    }
    else {
      val lpoints = readPointsFromHDFS(slicesPath + lid)
      val rpoints = readPointsFromHDFS(slicesPath + rid)
      Algorithm.bipartiteMST(lpoints, rpoints)
    }
  }

  def selfAdaptiveReduce(subMsts: RDD[Array[Edge]], k: Int, factor: Double = 0.5): RDD[Array[Edge]] = {
    var scale = k

    var partiallyAggregated = subMsts.mapPartitions{
      iter => Iterator(iter.reduce(Algorithm.kruskalReducer2))
    }
    var numPartitions = subMsts.getNumPartitions

    while (numPartitions > 1) {
      numPartitions /= scale
      val curNumPartitions = numPartitions

      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
        (i, iter) => iter.map((i, _))
      }.reduceByKey(new HashPartitioner(curNumPartitions), Algorithm.kruskalReducer(_, _)).values

      scale = Math.max(2, math.ceil(scale * factor).toInt)
    }

    partiallyAggregated
  }

  def adaptiveMultiWayReduce(subMsts: RDD[Array[Edge]], k: Int, factor: Double = 0.5): RDD[Array[Edge]] = {
    var scale = k
    var partiallyAggregated = subMsts.mapPartitions(it => Iterator(Algorithm.multiKruskalReducer(it)))
    var numPartitions = partiallyAggregated.getNumPartitions

    while (numPartitions > 1) {
      numPartitions /= scale
      val curNumPartitions = numPartitions

      partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
        case (i, iter) => iter.map((i, _))
      }.partitionBy(new HashPartitioner(curNumPartitions))
        .values.mapPartitions(it => Iterator(Algorithm.multiKruskalReducer(it)))

      scale = Math.max(2, math.ceil(scale * factor).toInt)
    }

    partiallyAggregated
  }

  def mstToPairs(mst: Seq[Edge], nClusters: Int): Iterable[(Long, Long)] = {
    val uf = new QuickUnionFind()
    val len = mst.length
    var i = 0

    // Union
    val iter = mst.iterator
    while (iter.hasNext) {
      val edge = iter.next()
      uf.add(edge.from)
      uf.add(edge.to)

      if (i < (len - nClusters + 1))
        uf.union(edge.from, edge.to)

      i += 1
    }

    // Get center
    uf.pairs()
  }

  def mstToLabel(mst: RDD[Array[Edge]], nClusters: Int): RDD[(Long, Int, Long)] = {
    val parsRDD = mst.flatMap(mstToPairs(_, nClusters)).cache()
    val labelsMap = parsRDD.map(_._2).distinct().collect().sorted.zipWithIndex.toMap

    val sc = mst.sparkContext
    val bcLabelsMap = sc.broadcast(labelsMap)

    parsRDD.mapPartitions { pairs =>
      val map = bcLabelsMap.value
      pairs.map { case (id, center) => (id, map(center), center) }
    }
  }

  def execute(points: RDD[Point]): RDD[(Long, Int, Long)] = {
    val sc = points.sparkContext

    var startTick = System.currentTimeMillis()
    logger.info("Phase 2 start, splitting points...")

    val hdfsConf = sc.hadoopConfiguration
    val HADOOP_HOME = sys.env("HADOOP_HOME")
    hdfsConf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"))
    hdfsConf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"))
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsConf)
    if (fs.exists(new Path(slicesPath))) fs.delete(new Path(slicesPath), true)

    // Split
    points.map{point => (point.id % s, point)}
      .partitionBy(new HashPartitioner(s))
      .saveAsHadoopFile(slicesPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    logger.info(f"Phase 2 end in ${(System.currentTimeMillis() - startTick) / 1000.0}%.2f s")

    // Partition
    startTick = System.currentTimeMillis()
    logger.info("Phase 3 start, partitioning...")

    val numSubgraphs: Int = s + s*(s-1)/2
    val partitionsRDD = createPartition(this.s, numSubgraphs, sc)

    logger.info(f"Phase 3 end in ${(System.currentTimeMillis() - startTick) / 1000.0}%.2f s")

    // Local MST
    startTick = System.currentTimeMillis()
    logger.info("Phase 4 start, computing local MST...")

    val subMSTsRDD = partitionsRDD.map(localMST).cache()
    subMSTsRDD.count()

    logger.info(f"Phase 4 end in ${(System.currentTimeMillis() - startTick) / 1000.0}%.2f s")

    startTick = System.currentTimeMillis()
    logger.info("Phase 5 start, adaptive k-way tree reducing...")

    // 1.Tree reduce
//    var h = (Math.log(numSubgraphs) / Math.log(k)).toInt  //log_k(numSubgraphs)
//    h = Math.max(h, 2)
//    val finalMST = subMSTsRDD.treeReduce(Algorithm.kruskalReducer2, h)
//    println(finalMST.length)

    // 2.Simple reduce
//    val finalMST = subMSTsRDD.reduce(Algorithm.kruskalReducer2)

    // 3.Adaptive reduce
//    val finalMST = selfAdaptiveReduce(subMSTsRDD, k).persist()

    // 4.Adaptive k-way reduce
    val finalMST = adaptiveMultiWayReduce(subMSTsRDD, k).persist(StorageLevel.MEMORY_AND_DISK)

    val finalNumEdges = finalMST.collect().map(_.length).sum
    subMSTsRDD.unpersist()

    logger.info(f"Phase 5 end in ${(System.currentTimeMillis() - startTick) / 1000.0}%.2f s")
    logger.info(s"final numEdges = $finalNumEdges")

    // Store result to HDFS
    if (fs.exists(new Path(outputFilePath))) fs.delete(new Path(outputFilePath), true)
//    sc.parallelize(finalMST).saveAsTextFile(outputFilePath)
//    finalMST.flatMap(x => x).saveAsTextFile(outputFilePath)

    // Get label
    mstToLabel(finalMST, this.nClusters).sortBy(_._1)
  }
}
