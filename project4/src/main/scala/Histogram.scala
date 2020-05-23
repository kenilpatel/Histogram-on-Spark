import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.File 
import java.io.PrintWriter
object Histogram {

  def main ( args: Array[ String ] ) {
    var conf = new SparkConf().setAppName("Histogram")
    var sc = new SparkContext(conf)
    var color = sc.textFile(args(0)).map( line => { val l = line.split(",")
                                                (l(0).toInt,l(1).toInt,l(2).toInt) } )
    var red=color.map(x=>{((x._1,1),1)})
    var blue=color.map(x=>{((x._2,2),1)})
    var green=color.map(x=>{((x._3,3),1)})  
    var interlist=red.union(blue)
    var colorlist=interlist.union(green)
    var cl=colorlist.reduceByKey(_+_)
    var textres=cl.map(x=>{x._1._2+"\t"+x._1._1+"\t"+x._2})  
    textres.collect().foreach(println)
  }
}
