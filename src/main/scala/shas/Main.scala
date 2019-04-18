package shas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Point

object Main {

  var inputFileName = "hdfs://localhost:9000//user/luoyl/shas/iris.txt"
  var outputFilePath = "hdfs://localhost:9000//user/luoyl/shas/iris-label/"
  var s = 2
  var k = 2
  var numClusters = 20

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HierarchicalClustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val logger = Logger.getLogger("SHAS")
    logger.setLevel(Level.INFO)

    if (args.length >= 5) {
      this.inputFileName = args(0)
      this.outputFilePath = args(1)
      this.s = args(2).toInt
      this.k = args(3).toInt
      this.numClusters = args(4).toInt
    } else {
      logger.error("arguments error")
      logger.error("Usage: xx.jar inputFileName outputFilePath S K numClusters")
    }

    val startTick = System.currentTimeMillis()
    logger.info(s"Phase 1 start, loading data from $inputFileName...")

    val hc = new HierarchicalClustering(s, k, outputFilePath, numClusters)

    val pointsRDD = sc.textFile(inputFileName, s).zipWithIndex.map{ case (line, id) =>
      new Point(id, line.split(",| ").map(_.toDouble))
    }.cache()

    logger.info(s"numPartitions: ${pointsRDD.getNumPartitions}")
    logger.info(f"Phase 1 end in ${(System.currentTimeMillis() - startTick) / 1000.0}%.2f s")

    val labels = hc.execute(pointsRDD).map(_._2).collect()
    labels.foreach(println)

    logger.info(f"Total time ${(System.currentTimeMillis() - startTick) / 1000.0}%.2f s")

    sc.stop()
  }

}
