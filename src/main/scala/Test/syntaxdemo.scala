package Test

object syntaxdemo {


import scala.util.matching.Regex


  def main(args: Array[String]) {
    val pattern = new Regex("\\(\\d+,\\)")
    val str = "ablaw is (123,) and (78,) cool"

    println((pattern findAllIn str).mkString(" "))
  }
}