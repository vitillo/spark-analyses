import scala.math._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.RangePartitioner

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import org.json4s._
import org.json4s.jackson.JsonMethods._

import Mozilla.Telemetry._

// 'export _JAVA_OPTIONS="-XmxNg"' to increase memory for a local cluster

object Analysis{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mozilla-telemetry").setMaster("local[*]").set("spark.local.dir", "/tmp")
    implicit val sc = new SparkContext(conf)
    implicit lazy val formats = DefaultFormats

    val pings = Pings("Firefox", "nightly", "36.0a1", "*", "20141106").RDD(0.1)
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

    val stacks = threadHangs.flatMap(hang => {
      val JObject(bins) = hang \ "histogram" \ "values"
      val JArray(rawStack) = hang \ "stack"
      val stack = rawStack.map(_.extract[String])
      var isPlugin = false

      try{
        for(frame <- stack){
          if(frame.startsWith("IPDL::PPlugin")) // PPluginModule, PPluginInstance, ...
            throw new Exception()
        }
      } catch {
        case e: Exception => isPlugin = true
      }

      val time = bins.flatMap{
        case (bin, JInt(cnt)) => {
          val x = 0.5*(bin.toInt + bin.toInt*2)
          val stacks = Iterator.fill(cnt.toInt)(stack).toList
          val times = Iterator.fill(cnt.toInt)(x).toList
          val plugins = Iterator.fill(cnt.toInt)(isPlugin).toList
          stacks.zip(times).zip(plugins)
        }
      }

      time
    })

    val pluginStacks = stacks.filter{ case ((stack, time), isPlugin) => {isPlugin} }
    val otherStacks = stacks.subtract(pluginStacks)

    val numberOfTotalStacks = stacks.count
    val numberOfPluginStacks = pluginStacks.count
    val numberOfOtherStacks = otherStacks.count

    val pluginStacksRatio = numberOfPluginStacks.toDouble/numberOfTotalStacks

    val pluginMedianPos = numberOfPluginStacks/2
    val pluginMedian = pluginStacks.sortBy(x => x._2).zipWithIndex().filter(_._2 == pluginMedianPos).first()._1._1._2

    val otherMedianPos = numberOfOtherStacks/2
    val otherMedian = otherStacks.sortBy(x => x._2).zipWithIndex().filter(_._2 == otherMedianPos).first()._1._1._2

    val topPluginStacks = pluginStacks.map(_._1._1).countByValue.toSeq.sortBy(- _._2).take(10).map{ case (stack, count) => {
      (stack, 100.0*count/numberOfPluginStacks)
    }}

    val topPluginClusters = pluginStacks.map{ case((stack, median), plug) => {
      (stack.last, 1)
    }}.countByKey.toSeq.sortBy(- _._2).take(10).map{ case(frame, count) => {
      (frame, 100.0*count/numberOfPluginStacks)
    }}

    println("Number of pings analyzed " + processHangs.count)
    println("Flash stack ratio: " + pluginStacksRatio)
    println("Number of plugin stacks: " + numberOfPluginStacks)
    println("Number of non-plugin stacks: " + numberOfOtherStacks)
    println("Median hang duration for plugin stacks: " + pluginMedian)
    println("Median hang duration for non-plugin stacks: " + otherMedian)

    println("Top plugin stacks: ")
    topPluginStacks.foreach{ case(stack, freq) => println(stack + " " + freq + " %")}

    println("Top plugin clusters grouped by the last frame: ")
    topPluginClusters.foreach{ case(frame, freq) => println(frame + " " + freq + " %")}

    sc.stop()
  }
}
