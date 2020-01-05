package Util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object GetPaperAbstractRDD {

  def work(filename:String , sc: SparkContext): DataFrame = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "PAPER") // Required
      .option("useHeader", "true") // Required
//      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
//      .option("inferSchema", "false") // Optional, default: false
//      .option("addColorColumns", "true") // Optional, default: false
//      .option("startColumn", 0) // Optional, default: 0
//      .option("endColumn", 99) // Optional, default: Int.MaxValue
//      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
//      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
//      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      //.schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load(filename)  //加载表文件


    df


  }
}



