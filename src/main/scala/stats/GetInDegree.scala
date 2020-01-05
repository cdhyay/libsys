package stats

import Util.EdgeUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GetInDegree {
  /**
    * 主方法
    *
    * @param args 命令行参数
    */
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf()
      .setAppName("GetInDegree")

    val sc = new SparkContext(conf)

    val sourcePath: String = "/home/hcd/data/graph/edges/"
    val resultPath: String = "/home/hcd/result/"

    val vertices: RDD[(VertexId, Int)] =
      sc.textFile("/home/hcd/data/txt/all.txt")
        .map(_.toLong -> 0)


    val piapiGraph: Graph[Int, Int] = Graph(vertices,
      sc.textFile(sourcePath + "edgesOfPaperPaperGraphByIndexTermAndAbstract.txt").map(EdgeUtil.stringToEdge_Double))
    val indOfpiapi: RDD[(VertexId, Int)] = vertices
      .fullOuterJoin(piapiGraph.inDegrees)
      .map(tuple => {
        tuple._1 -> tuple._2._2.getOrElse(0)
      })
    indOfpiapi.saveAsTextFile(resultPath + "indOfpiapi")
    piapiGraph.unpersist()
    indOfpiapi.unpersist()

    sc.stop()

  }
}
