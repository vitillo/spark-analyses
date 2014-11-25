import scala.math._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.RangePartitioner

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.clustering.KMeans

import org.json4s._
import org.json4s.jackson.JsonMethods._

import Mozilla.Telemetry._

// 'export _JAVA_OPTIONS="-XmxNg"' to increase memory for a local cluster

object Analysis{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mozilla-telemetry").setMaster("local[*]").set("spark.local.dir", "/tmp")
    implicit val sc = new SparkContext(conf)
    implicit lazy val formats = DefaultFormats

    val pings = Pings("Firefox", "nightly", "*", "*", "20141106").RDD(0.1)
    val processHangs = pings.map(ping => parse(ping.substring(37)) \ "threadHangStats").cache

    val threadHangs = processHangs.flatMap{
      case JArray(list) => list
      case _ => Nil
    }.filter(threadHangs =>
      threadHangs \ "name" == JString("Gecko")
    ).flatMap(threadHangs => {
      val JArray(list) = threadHangs \ "hangs"
      list
    })

    val framePattern = ("""(.*)(:[0-9].*)""").r
    val stacks = threadHangs.flatMap(hang => {
      val JObject(bins) = hang \ "histogram" \ "values"
      val JArray(s) = hang \ "stack"

      val stack = s.map(frame => {
        val f = frame.extract[String]
        f match {
          case framePattern(filename, line) => filename
          case _ => f}
      })

      val stacks = bins.flatMap{
        case (bin, JInt(cnt)) => {
          val stacks = Iterator.fill(cnt.toInt)(stack).toList
          stacks
        }
        case _ => Nil
      }

      stacks
    }).filter(!_.isEmpty)

    // Aggregate stacks by their top frame and get the top K
    val topFrames = stacks.groupBy { stack => {
      stack.last
    }}.sortBy(- _._2.size).zipWithIndex().filter { case(x, y) =>{
      y < 15 && y > 0
    }}

    val numberOfStacks = stacks.count.toDouble

    // Prepare some summaries about the top N stacks of the hottest frames
    val summary = topFrames.map(topFrame => {
      val ((frame, stacks), idx) = topFrame
      val count = stacks.size.toDouble
      val top = stacks.groupBy(x => x).map(x => (x._1, x._2.size / count)).toList.sortBy(- _._2).take(3)
      (frame, count, top)
    })

    // Fun times
    for(in <- summary.collect){
      val (frame, count, topStacks) = in
      println("Frame " + frame + " accounts for " + count/numberOfStacks + " of all top frames; the top stacks are: ")
      topStacks.map(println)
      println("=====================================")
    }

    sc.stop()
  }
}
