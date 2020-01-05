package Test

import Util.GetPaperAbstractRDD
import org.ansj.library.{DicLibrary, StopLibrary}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.{BaseAnalysis, DicAnalysis, NlpAnalysis, ToAnalysis}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object DataPartition_Test {
  def main (args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress","False")


    //----添加自定义词典----
    val dicfile:String = "ExtendDic.txt" //ExtendDic为一个文本文件的名字，里面每一行存放一个词
    for (word <- Source.fromFile(dicfile).getLines) { DicLibrary.insert(DicLibrary.DEFAULT,word)} //逐行读入文本文件，将其添加到自定义词典中
    println("done")

    def replaceChars(str: String): String = {
      val res = str.replaceAll("(\0|\\s*|\r|\n)", "")
      res
    }

    //----添加停用词----
    val filter = new StopRecognition()
    //自定义添加停用词
    val stopWordFile:String = "stopword.dic"

    for (word <- Source.fromFile(stopWordFile).getLines) { filter.insertStopWords(word)} //逐行读入文本文件，将其添加到停用词词典中
    println("done")
    //过滤掉标点
    filter.insertStopNatures("w")
    filter.insertStopRegexes("\\s")


    // ----构建spark对象----
    val conf = new SparkConf().setAppName("TextClassificationDemo").setMaster("local")
    val sc = new SparkContext(conf)
    println("done")

    val paper_id_abstract: String = "paper01.xlsx"
    val df1 = GetPaperAbstractRDD.work(paper_id_abstract,sc)
    //df1.map(row => (row(0),BaseAnalysis.parse(row(1).).recognition(filter).toStringWithOutNature(" ")))
    val abstractinfo = df1.collect()
    val paperAbstract = sc.parallelize(abstractinfo)
    val tuple1:RDD[(String,String)] = paperAbstract.map(x => {
      if(x(1)!=null)
    (x(0).toString,x(1).toString)
    else
    (x(0).toString," ")
  })
  //分词结果显示

    val splited = tuple1.map(x => (x._1,NlpAnalysis.parse(x._2).recognition(filter).toStringWithOutNature(" ")))
    //val splited = paperAbstractRDD.map(x => NlpAnalysis.parse(x).recognition(filter).toStringWithOutNature("|"))
    splited.saveAsTextFile( "partedresult")

    sc.stop()

  }
}