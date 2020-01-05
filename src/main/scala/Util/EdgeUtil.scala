package Util



import Conf.Conf
import org.apache.spark.graphx.Edge

/**
  * 边的工具类
  */
object EdgeUtil {
  /**
    * 使所有Edge的srcId比dscId小
    *
    * @param srcId 起点顶点编号
    * @param dstId 目的顶点编号
    * @return Edge[Int]
    */
  def SortEdge(srcId: Int, dstId: Int): Edge[Int] = {
    if (srcId < dstId) {
      Edge(srcId.toLong, dstId.toLong, 1)
    } else {
      Edge(dstId.toLong, srcId.toLong, 1)
    }
  }

  /**
    * 将边转为字符串，并将论文id（已做偏移处理）转为原论文id
    *
    * @param edge     需要格式化成字符串的边
    * @param edgeType 需要格式化成字符串的边的类型
    * @return String
    */
  def edgeToString(edge: Edge[Int], edgeType: Int): String = {
    var srcId: String = ""
    var dstId: String = ""
    if (edgeType == 0) {
      //图书与图书
      srcId = edge.srcId.toString
      dstId = edge.dstId.toString
    } else if (edgeType == 1) {
      //论文与论文
      srcId = (edge.srcId - Conf.paperIdOffset).toString
      dstId = (edge.dstId - Conf.paperIdOffset).toString
    } else {
      //图书与论文
      srcId = edge.srcId.toString
      dstId = (edge.dstId - Conf.paperIdOffset).toString
    }
    //格式化字符串
    s"$srcId,$dstId,$edgeType"
  }

  /**
    * 确定边的类型
    *
    * 图书与图书0
    * 论文与论文1
    * 图书与论文2
    *
    * @param edge 需要确定类型的边
    * @return Int
    */
  def getEdgeType(edge: Edge[Int]): Int = {
    //注：本程序所有的edge的srcId小于dstId

    //类型
    var typeResult = -1
    if (edge.dstId < Conf.paperIdOffset) {
      //图书与图书
      typeResult = 0
    }
    else if (edge.srcId > Conf.paperIdOffset) {
      //论文与论文
      typeResult = 1
    }
    else {
      //图书与论文
      typeResult = 2
    }
    //返回类型
    typeResult
  }

  /**
    * 字符串转为边
    *
    * @param string 需要转为边的字符串
    * @return 边
    */
  def stringToEdge(string: String): Edge[Int] = {
    //去除多余的字符并分割
    val tokens: Array[String] = string.replace("Edge(", "").replace(")", "").split(",")
    val long01: Long = tokens.head.toLong
    val long02: Long = tokens(1).toLong
    val int01: Int = tokens(2).toInt
    //返回边
    if (long01 < long02) {
      Edge(long01, long02, int01)
    } else {
      Edge(long02, long01, int01)
    }
  }

  def stringToEdge_Double(string: String): Edge[Int] = {
    //去除多余的字符并分割
    val tokens: Array[String] = string.replace("(", "").replace(")", "").split(",")
    val long01: Long = tokens.head.toLong
    val long02: Long = tokens(1).toLong
    val int01: Int =  10000000 * tokens(2) .toInt
    //返回边
    if (long01 < long02) {
      Edge(long01, long02, int01)
    } else {
      Edge(long02, long01, int01)
    }
  }
}
