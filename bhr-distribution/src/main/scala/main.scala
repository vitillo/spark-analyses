import java.io._

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

    val pings = Pings("Firefox", "nightly", "*", "*", "20141106").RDD(0.2)
    val processHangs = pings.map(ping => parse(ping.substring(37)) \ "threadHangStats").cache

    val sessionHangs = processHangs.flatMap{
      case JArray(list) => list
      case _ => Nil
    }.filter(threadHangs =>
      threadHangs \ "name" == JString("Gecko")
    ).map(threadHangs => {
      val JArray(hangs) = threadHangs \ "hangs"

      hangs.map(hang => {
        val JObject(bins) = hang \ "histogram" \ "values"
        val sum = bins.map{ case (bin, JInt(cnt)) => cnt }.sum
        assert(sum > 0)
        sum
      }).sum.toDouble
    })

    var writer = new PrintWriter(new File("session.csv" ))
    sessionHangs.collect.foreach(x => writer.println(x))
    writer.close()

    sc.stop()
  }
}
