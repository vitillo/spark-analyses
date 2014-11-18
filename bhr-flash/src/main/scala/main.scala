import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.json4s._
import org.json4s.jackson.JsonMethods._

import Mozilla.Telemetry._

object Analysis{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mozilla-telemetry").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)
    implicit lazy val formats = DefaultFormats

    val pings = Pings("Firefox", "nightly", "36.0a1", "*", ("20141106")).RDD(0.3)
    val processHangs = pings.map(ping => parse(ping.substring(37)) \ "threadHangStats").cache

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
        case _ => 0.toInt
      }.sum

      val time = bins.map{
        case (bin, JInt(cnt)) => 0.5*(bin.toInt + bin.toInt*2)*cnt.toInt
        case _ => 0.toInt
      }.sum

      (hang \ "stack", count, time)
    })

    val pluginStacks = stacks.filter{ case (JArray(stack), count, time) => {
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

    val pluginStacksRatio = pluginStacks.map(_._2).sum/stacks.map(_._2).sum
    val pluginStacksTimeRatio = pluginStacks.map(_._3).sum/stacks.map(_._3).sum

    val pluginFrames = stacks.flatMap{ case (JArray(stack), count, time) => {
      stack.filter(frame => frame.extract[String].startsWith("IPDL::PPlugin"))
    }}.distinct.collect

    println("Flash stack ratio: " + pluginStacksRatio)
    println("Flash timing ratio: " + pluginStacksTimeRatio)
    println("Flash frames considered:")
    println(pluginFrames.foreach(println))

    sc.stop()
  }
}
