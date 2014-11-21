import scala.math._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

import org.json4s._
import org.json4s.jackson.JsonMethods._

import Mozilla.Telemetry._

// 'export _JAVA_OPTIONS="-XmxNg"' to increase memory for a local cluster

object Analysis{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mozilla-telemetry").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)
    implicit lazy val formats = DefaultFormats

    val pings = Pings("Firefox", "nightly", "36.0a1", "*", "20141106").RDD(0.5)
    val processHangs = pings.map(ping => parse(ping.substring(37)) \ "threadHangStats").cache
    // val processHangs = pings.filter(_.contains("flashVersion")).map(ping => parse(ping.substring(37)) \ "threadHangStats").cache
    // removing pings without flashVersion has basically no impact whatsoever on
    // total number of plugin stacks, not that it means really much...

    val threadHangs = processHangs.flatMap{ case JArray(list) =>
      list
    }.filter(threadHangs =>
      threadHangs \ "name" == JString("Gecko")
    ).flatMap(threadHangs => {
      val JArray(list) = threadHangs \ "hangs"
      list
    })

    val stacks = threadHangs.map(hang => {
      val JObject(bins) = hang \ "histogram" \ "values"

      val count = bins.map{
        case (bin, JInt(cnt)) => cnt.toInt
        case _ => 0
      }.sum

      val time = bins.map{
        case (bin, JInt(cnt)) => {
          val x = 0.5*(bin.toInt + bin.toInt*2)
          val x2 = x*x
          (cnt.toInt*x, cnt.toInt*x2)
        }
        case _ => (0.0, 0.0)
      }

      (hang \ "stack", count, time.map(_._1).sum, time.map(_._2).sum)
    })

    val pluginStacks = stacks.filter{ case (JArray(stack), count, time, timeSquared) => {
      var found = false

      try{
        for(frame <- stack){
          val name = frame.extract[String]

          if(name.startsWith("IPDL::PPlugin")) // PPluginModule, PPluginInstance, ...
            throw new Exception()
        }
      } catch {
        case e: Exception => found = true
      }

      found
    }}

    val otherStacks = stacks.subtract(pluginStacks)

    val numberOfTotalStacks = stacks.map(_._2).sum
    val numberOfPluginStacks = pluginStacks.map(_._2).sum
    val numberOfOtherStacks = otherStacks.map(_._2).sum

    val pluginStacksRatio = numberOfPluginStacks/numberOfTotalStacks

    val expectedPlugin = pluginStacks.map(_._3).sum/numberOfPluginStacks
    val deviationPlugin = sqrt(pluginStacks.map(_._4).sum/numberOfPluginStacks - pow(expectedPlugin, 2))
    val errorPlugin = deviationPlugin / sqrt(numberOfPluginStacks)

    val expectedOther = stacks.map(_._3).sum/numberOfOtherStacks
    val deviationOther = sqrt(otherStacks.map(_._4).sum/numberOfOtherStacks - pow(expectedOther, 2))
    val errorOther = deviationOther / sqrt(numberOfOtherStacks)

    val pluginFrames = stacks.flatMap{ case (JArray(stack), count, time, timeSquared) => {
      stack.filter(frame => frame.extract[String].startsWith("IPDL::PPlugin"))
    }}.distinct.collect

    println("Number of pings analyzed " + processHangs.count)
    println("Flash stack ratio: " + pluginStacksRatio)
    println("Number of plugin stacks: " + numberOfPluginStacks)
    println("Number of non-plugin stacks: " + numberOfOtherStacks)
    println("Expected hang duration in ms for plugin stack " + expectedPlugin + " +- " + errorPlugin)
    println("Expected hang duration in ms for non-plugin stack " + expectedOther + " +- " + errorOther)
    println("Flash frames considered:")

    println(pluginFrames.foreach(frame => println(frame.extract[String])))

    sc.stop()
  }
}
