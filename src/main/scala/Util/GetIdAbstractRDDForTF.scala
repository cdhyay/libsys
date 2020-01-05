package Util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

object GetIdAbstractRDDForTF {

  def work(paper_id_abstract_splited: String, sc: SparkContext): RDD[(Seq[String], Long)] = {
    //分割符
    val delimiter01 = ","
    //返回RDD
    sc.textFile(paper_id_abstract_splited)
      .filter(isNull(_))    //去除没有摘要的行
      .filter(_.trim.length > 0)   //去除空行
      .map(line => {
        val tokens = line
          .replace("(","")
          .replace(")","")
          .split(delimiter01)
          .map(_.trim)

        tokens(1).split(" ").toSeq -> tokens(0).toLong

      })
  }

  def isNull(str: String) :Boolean =  {

    if(str.matches("\\(\\d+,\\)"))
      false
    else
      true
  }

}
