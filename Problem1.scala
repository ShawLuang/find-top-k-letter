
package p3
  
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source


object Problem1 {
  def main (args: Array[String]){
    val inputFile = args(0)
    val outputFolder = args(1)
    val top = args(2).toInt
    val conf = new SparkConf().setAppName("find top k term").setMaster("local");
    val sc = new SparkContext(conf);
    
    val input = sc.textFile(inputFile)
    val file_content = input.collect()
    var temp:List[String] = List[String]()
      
    for (line <- file_content){ 
      val line_split = line.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").toList.filterNot { _.isEmpty() }.distinct
      for (word <- line_split) {
        if(Character.isLetter(word.charAt(0)))
        temp ::= word.toLowerCase()
      }
    }

    /*concert it as RDDfile*/
    val wordcount = sc.parallelize(temp).map(word => (word, 1)).reduceByKey(_+_)
    /* concert RDDfile to avaFile */
    val c_counts = wordcount.collect()

    val sorted = c_counts.sortBy( x => (-x._2, x._1))
    var r:List[(String, Int)] = List()
    for (i <- 0 to top){
      r ::= sorted(i)
    }
    
    
    val result= sc.parallelize(r).map(x => x)
    result.saveAsTextFile(outputFolder)
    
   
  }
  
  
}
