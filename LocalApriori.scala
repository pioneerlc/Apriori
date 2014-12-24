import org.apache.spark.rdd.RDD

/**
 * Created by clin3 on 2014/12/23.
 */
object LocalApriori {
  def generateFrequent1Itemsets(
      data: Array[Array[String]],
      minSupport: Int): Array[(Array[String], Int)] = {
    data.flatMap(record => record.map(item => (item, 1)))
      .foldLeft(Map[String, Int]())((map, pair) => {
        if(map.contains(pair._1)){
          map.updated(pair._1, map(pair._1) + pair._2)
        } else {
          map + pair
        }
    }).filter(_._2 >= minSupport).map(pair => (Array(pair._1), pair._2)).toArray
  }

  def aprioriGen(frequentItemsets: Array[(Array[String], Int)], k: Int): Array[Array[String]] = {
    frequentItemsets.flatMap(x => frequentItemsets.flatMap(y => {
      var i = 0
      var pos = -1
      var break = false
      while(i < k && !break) {
        if(x._1(i) != y._1(i)){
          pos = i
          break = true
        }
        i = i + 1
      }
      if(pos == k - 1 && x._1(pos) < y._1(pos)) {
        Some(x._1.slice(0, k) :+ y._1(k - 1))
      } else {
        None
      }
    }))
  }

  def generateFrequentItemset(
      data: Array[Array[String]],
      candidateItemsets: Array[Array[String]],
      minSupport: Int): Array[(Array[String], Int)] = {
    candidateItemsets.map(itemset => {
      data.map(record => {
        var notFound = false
        for(item <- itemset) {
          if(!record.contains(item))
            notFound = true
        }
        if(notFound) {
          (itemset.sortWith(_.toInt < _.toInt), 0)
        } else {
          (itemset.sortWith(_.toInt < _.toInt), 1)
        }
      }).reduce((x, y) => (x._1, x._2 + y._2))
    }).filter(_._2 >= minSupport)
  }

  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String): Array[(Array[String], Int)] ={
    val minSupport = (supportThreshold * data.count()).toInt
    val localData = data.map(line => line.split(splitterPattern)).collect()

    var k = 1
    var frequentItemsets = generateFrequent1Itemsets(localData, minSupport)
    var results = frequentItemsets

    while(frequentItemsets.length != 0) {
      k = k + 1
      val candidateItemsets = aprioriGen(frequentItemsets, k - 1)
      frequentItemsets = generateFrequentItemset(localData, candidateItemsets, minSupport)
      results ++= frequentItemsets
    }
    results
  }
}
