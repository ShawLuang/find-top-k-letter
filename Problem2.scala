package p3

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source

object Problem2 {
  def main (arg: Array[String]){

    val inputFile = arg(0)
    val outputFolder = arg(1)
    val conf = new SparkConf().setAppName("reverse graph edge").setMaster("local");
    val sc = new SparkContext(conf);
    val input = sc.textFile(inputFile)
    val input_Array = input.collect()
    
    val graph = input_Array.map{ x => (x.split("\t")(0), x.split("\t")(1))}.map{ x => (x._2.toInt , x._1)}
    val graphRDD = sc.parallelize(graph).reduceByKey((x,y) => x + "," + y)
    
    val graphNormal = graphRDD.collect().sortBy(_._1)
    
    var output_string = ""
    for (i <- graphNormal) {
      var temp = ""
      var symble = ""
      val s = i._2.split(",").map { x => x.toInt }.sorted
      for (j <- s) {
        temp =  temp + symble + j.toString
        symble = ","
      }
      output_string = output_string + i._1 + "\t" + temp + "\n"
      
    }
    
    val result = sc.parallelize(output_string.split("\n")).map{x => x}
    result.saveAsTextFile(outputFolder)
  }
}