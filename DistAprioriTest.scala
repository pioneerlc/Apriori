import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.compat.Platform._

/**
 * Created by clin3 on 2014/12/23.
 */

object DistAprioriTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("spark://sr471:7177")
      .setAppName("distApriori")
      .set("spark.cores.max", "128")
      .set("spark.executor.memory", "24G")

    val sc = new SparkContext(conf)

    val supportThreshold = args(0).toDouble
    val fileName = args(1)
    val splitterPattern = args(2)
    val optimization = args(3)
    val degree = args(4).toInt

    val startTime = currentTime
    val data = sc.textFile("hdfs://sr471:54311/user/clin/apriori/input/" + fileName, 128)
    val frequentItemsets = DistApriori.run(data, supportThreshold, splitterPattern, optimization, degree)
    val count = frequentItemsets.count
    val endTime = currentTime
    val totalTime: Double = endTime - startTime



    println("---------------------------------------------------------")
    println("This program totally took " + totalTime/1000 + " seconds.")
    println("Number of frequent itemsets using DistApriori= " + count)
    println("---------------------------------------------------------")

  }
}
