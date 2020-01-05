package Test

import Util.EdgeUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

object GetInDegreeTest {
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
      .setMaster("local")

    val sc = new SparkContext(conf)



    val vertices: RDD[(VertexId, Int)] =
      sc.textFile(" all.txt")
        .map(_.toLong -> 0)


    val piapiGraph: Graph[Int, Int] = Graph(vertices,
      sc.textFile( "edges.txt").map(EdgeUtil.stringToEdge_Double))
    val indOfpiapi: RDD[(VertexId, Int)] = vertices
      .fullOuterJoin(piapiGraph.inDegrees)
      .map(tuple => {
        tuple._1 -> tuple._2._2.getOrElse(0)
      })
    indOfpiapi.foreach(println)
    piapiGraph.unpersist()
    indOfpiapi.unpersist()

    sc.stop()

  }

}
