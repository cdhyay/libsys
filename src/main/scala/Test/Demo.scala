package Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex


object Demo {

  def main (args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "False")
    // ----构建spark对象----
    val conf = new SparkConf().setAppName("TextClassificationDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val documents = sc.textFile("word.txt").filter(isNull(_))

    documents.foreach(println)

    sc.stop()
  }

  def isNull(str: String) :Boolean =  {
    val pattern = new Regex("\\(\\d+,\\)")
    if(str.matches("\\(\\d+,\\)"))
      false
    else
      true
  }



}
