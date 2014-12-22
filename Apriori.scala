/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform.currentTime
import org.apache.spark.SparkContext._
import scala.util.control._

/**
 * Created by clin3 on 2014/11/25.
 */

class Apriori private (
  private var supportThreshold: Double,
  private var splitterPattern: String,
  private var optimization: Int,
  private var numSlices: Int) extends Serializable {

  def generateFrequent1Itemsets(data: Array[Array[String]], minSupport: Int): Array[(Array[String], Int)] = {
    data.flatMap(record => record.map(item => (item, 1))).foldLeft(Map[String, Int]())((map, pair) => {
      if(map.contains(pair._1)){
        map.updated(pair._1, map(pair._1) + pair._2)
      } else {
        map + pair
      }
    }).filter(_._2 >= minSupport).map(pair => (Array(pair._1), pair._2)).toArray
  }

  def aprioriGen(frequentItemsets: Array[(Array[String], Int)], k: Int): Array[Array[String]] = {
    frequentItemsets.flatMap(first => frequentItemsets.flatMap(second => {
      var i = 0
      var pos = -1
      var break = false
      while(i < k && !break) {
        if(first._1(i) != second._1(i)){
          pos = i
          break = true
        }
        i = i + 1
      }
      if(pos == k - 1 && first._1(pos) < second._1(pos)) {
        Some(first._1.slice(0, k) :+ second._1(k - 1))
      } else {
        None
      }
    }))
  }

  def generateFrequentItemset(
      transactions: Array[Array[String]],
      candidateItemsets: Array[Array[String]],
      minSupport: Int): Array[(Array[String], Int)] = {
    candidateItemsets.map(itemset => {
      transactions.map(transaction => {
        var not_found = false
        for(item <- itemset) {
          if(!transaction.contains(item))
            not_found = true
        }
        if(not_found) {
          (itemset.sortWith(_ < _), 0)
        } else {
          (itemset.sortWith(_ < _), 1)
        }
      }).reduce((x, y) => (x._1, x._2 + y._2))
    }).filter(_._2 >= minSupport)
  }

  def sequentialApriori(transactions: Array[Array[String]], minSupport: Int, outputPath: String): Unit ={
    var k = 1
    var count = 0
    val pw = new PrintWriter(new FileWriter(outputPath, true))
    var frequentItemsets = generateFrequent1Itemsets(transactions, minSupport)
    while(frequentItemsets.length != 0) {
      k = k + 1
      val candidateItemsets = aprioriGen(frequentItemsets, k - 1)
      frequentItemsets = generateFrequentItemset(transactions, candidateItemsets, minSupport)
      for(pattern <- frequentItemsets) {
        for(item <- pattern._1) {
          pw.write(item + " ")
        }
        pw.write(pattern._2 + "\r\n")
      }
      count = count + frequentItemsets.length
    }
    println("count = " + count)
    pw.close()
  }

  def parallelGenerateFrequent1Itemsets(
      transactions: RDD[String],
      minSupport: Int,
      split_pattern: String): RDD[(String, Int)] = {
    transactions.flatMap(record =>
      record.split(split_pattern).map(item => (item, 1))).reduceByKey(_ + _).filter(_._2 >= minSupport)
  }

  def parallelAprioriGen(
      sc: SparkContext,
      frequentItemsets: RDD[(String, Int)],
      k: Int,
      splitterPattern: String): RDD[String] = {
    val leftFrequentItemsets = frequentItemsets
    val rightRrequentItemsets = sc.broadcast(leftFrequentItemsets.collect())
    leftFrequentItemsets.flatMap(first => rightRrequentItemsets.value.flatMap(second => {
      var i = 0
      var pos = -1
      var break = false
      var ret_val = ""
      val leftItemset = first._1.split(splitterPattern)
      val rightItemset = second._1.split(splitterPattern)
      while(i < k && !break) {
        if(leftItemset(i) != rightItemset(i)){
          pos = i
          break = true
        }
        i = i + 1
      }
      if(pos == k - 1 && leftItemset(pos).toInt < rightItemset(pos).toInt) {
        ret_val = leftItemset.slice(0, pos + 1).mkString(splitterPattern)
        ret_val += splitterPattern
        ret_val += rightItemset(pos)
        Some(ret_val)
      } else {
        None
      }
    }))
  }

  /*
  def parallel_apriori_gen(sc: SparkContext, frequent_itemset: RDD[(String, Int)], k: Int, split_pattern: String): Array[String] = {
    val left_frequent_itemset = frequent_itemset.collect()
    val right_frequent_itemset = left_frequent_itemset
    left_frequent_itemset.flatMap(first => right_frequent_itemset.flatMap(second => {
      var i = 0
      var pos = -1
      var break = false
      var ret_val = ""

      val itemset_1 = first._1.split(split_pattern)
      val itemset_2 = second._1.split(split_pattern)

      while(i < k && !break) {
        if(itemset_1(i) != itemset_2(i)){
          pos = i
          break = true
        }
        i = i + 1
      }
      if(pos == k - 1 && itemset_1(pos) < itemset_2(pos)) {
        ret_val = itemset_1.slice(0, pos + 1).mkString(split_pattern)
        ret_val += split_pattern
        ret_val += itemset_2(pos)
        Some(ret_val)
      } else {
        None
      }
    }))
  }
  */


  def buildTrietree(candidateItemsets: Array[String], k: Int, splitterPattern: String): TrieTreeNode = {
    val trietreeRoot = new TrieTreeNode()
    var currentNode: Option[TrieTreeNode] = None
    var parentNode: Option[TrieTreeNode] = None
    for(currentItemset <- candidateItemsets) {
      parentNode = None
      currentNode = Some(trietreeRoot)
      for(item <- currentItemset.split(splitterPattern)) {
        val children = currentNode.get.children
        parentNode = currentNode
        if(children.contains(item)) {
          currentNode = children.get(item)
        } else {
          currentNode = Some(new TrieTreeNode())
          children.put(item, currentNode.get)
        }
        parentNode.get.children = children
      }
      currentNode.get.isLeafNode = true
      val itemsets = currentNode.get.itemsets
      currentNode.get.itemsets = (itemsets += currentItemset)
    }
    trietreeRoot
  }

  def print_trietree(trietreeRoot: TrieTreeNode): Unit = {
    if(trietreeRoot == None) {
    } else {
      if(trietreeRoot.isLeafNode == true){
        for(item <- trietreeRoot.itemsets){
          println(item)
        }
      } else {
        val children = trietreeRoot.children
        for(entry <- children) {
          print_trietree(entry._2)
        }
      }
    }
  }

  def traverseTrietree(
      trietreeRoot: TrieTreeNode,
      transaction: String,
      startIndex: Int,
      splitterPattern: String): ArrayBuffer[String] = {
    if(trietreeRoot.isLeafNode == true) {
      trietreeRoot.itemsets
    } else {
      val matchedItemsets = new ArrayBuffer[String]()
      val t = transaction.split(splitterPattern)
      for(i <- startIndex until t.length) {
        val item = t(i)
        val children = trietreeRoot.children
        if(children.contains(item)) {
          val itemset = traverseTrietree(children.get(item).get, transaction, i + 1, splitterPattern)
          matchedItemsets ++= itemset
        }
      }
      matchedItemsets
    }
  }

  def hash(key: Int): Int ={
    key % 4096
  }

  def buildHashtree(candidateItemsets: Array[String], k: Int, splitterPattern: String): HashTreeNode = {
    def findNextNode(p: HashTreeNode, items: Array[String]): HashTreeNode ={
      val h = hash(items(p.level).toInt)
      p.children(h)
    }
    def leafToBranch(p: HashTreeNode): Unit ={
      for(i <- 0 until 4096){
        p.children(i) = new HashTreeNode()
        p.children(i).level = p.level + 1
      }
      p.isLeafNode = false
      for(items <- p.candidateItemsets){
        val c = items.split(splitterPattern)
        val h = hash(c(p.level).toInt)
        val q = p.children(h)
        insert_itemsets(q, items)
      }
      p.candidateItemsets = new ArrayBuffer[String]()
    }
    def insert_itemsets(current_node: HashTreeNode, items: String): Unit ={
      current_node.candidateItemsets += items
    }
    val D = 4096
    val hashtreeRoot = new HashTreeNode()
    for(itemsets <- candidateItemsets) {
      val items = itemsets.split(splitterPattern)
      var current_node = hashtreeRoot
      while(current_node.isLeafNode == false){
        current_node = findNextNode(current_node, items)
      }
      while(current_node.candidateItemsets.length >= D && current_node.level < k){
        leafToBranch(current_node)
        current_node = findNextNode(current_node, items)
      }
      insert_itemsets(current_node, itemsets)
    }
    hashtreeRoot
  }

  def print_hashtree(hashtree_root: HashTreeNode): Unit ={
    if(hashtree_root == None) {

    } else{
      if(hashtree_root.isLeafNode == true){
        println("level = " + hashtree_root.level)
        for(item <- hashtree_root.candidateItemsets){
          println(item)
        }
      } else {
        val children = hashtree_root.children
        for(entry <- children) {
          print_hashtree(entry)
        }
      }
    }
  }

  def traverseHashtree(
      transaction: Array[String],
      p: HashTreeNode,
      splitterPattern: String,
      prefix: ArrayBuffer[String],
      k: Int): ArrayBuffer[String] ={
    def isSubset(c: String, transaction: Array[String], splitterPattern: String, prefix: ArrayBuffer[String]): Boolean ={
      val items = c.split(splitterPattern)
      var find = true
      val loop = new Breaks
      loop.breakable(
        for(i <- 0 until items.length){
          val item = items(i)
          if(i < prefix.length){
            if(item != prefix(i)) {
              find = false
              loop.break()
            }
          }else{
            if(!transaction.contains(item)){
              find = false
              loop.break()
            }
          }
        }
      )
      find
    }
    val res = new ArrayBuffer[String]()
    if(p.isLeafNode == true){
      for(c <- p.candidateItemsets){
        if(isSubset(c, transaction, splitterPattern, prefix) == true){
          res.append(c)
        }
      }
    } else {
      if(prefix.length < k && transaction.length >= 1){
        val subTransaction = transaction.slice(1, transaction.length)
        val q = p.children(hash(transaction(0).toInt))
        res ++= traverseHashtree(subTransaction, q, splitterPattern, prefix :+ transaction(0), k)
        res ++= traverseHashtree(subTransaction, p, splitterPattern, prefix, k)
      }
    }
    res
  }

  def parallelGenerateFrequentItemsets(
      sc: SparkContext,
      transactions: RDD[String],
      bcCandidateItemsets: Array[String],
      minSupport: Int,
      splitterPattern: String): RDD[(String, Int)] = {
    transactions.flatMap(transaction => {
      val itemset = transaction.split(splitterPattern)
      for(candidateItemset <- bcCandidateItemsets) yield {
        var not_found = false
        val splittedCandidateItemset = candidateItemset.split(splitterPattern)
        for(item <- splittedCandidateItemset) {
          if(!itemset.contains(item)) {
            not_found = true
          }
        }
        if(not_found) {
          (splittedCandidateItemset.sortWith(_.toInt < _.toInt).mkString(splitterPattern), 0)
        } else {
          (splittedCandidateItemset.sortWith(_.toInt < _.toInt).mkString(splitterPattern), 1)
        }
      }
    }).reduceByKey(_ + _).filter(_._2 >= minSupport)
  }

  def parallelApriori(
      sc: SparkContext,
      transactions: RDD[String],
      minSupport: Int,
      splitterPattern: String,
      optimization: Int,
      outputPath: String): Unit = {
    var k = 1
    var frequentItemsets = parallelGenerateFrequent1Itemsets(transactions, minSupport, splitterPattern).cache()
    frequentItemsets.saveAsTextFile(outputPath + k + "-itemsets/");
    var count = 0
    while(frequentItemsets.count() != 0) {
      k = k + 1
      val candidateItemsets = parallelAprioriGen(sc, frequentItemsets, k - 1, splitterPattern).collect()
      optimization match {
        case 1 => {
          val trietree = buildTrietree(candidateItemsets, k, splitterPattern)
          val bcTrietree = sc.broadcast(trietree)
          frequentItemsets = transactions.flatMap(transaction => {
            val t = transaction.split(splitterPattern).sortWith(_.toInt < _.toInt)
            val itemsets = traverseTrietree(bcTrietree.value, t.mkString(splitterPattern), 0, splitterPattern)
            itemsets.map(item => (item, 1))
          }).reduceByKey(_ + _).filter(_._2 >= minSupport).cache()
        }
        case 2 => {
          val hashtree = buildHashtree(candidateItemsets, k, splitterPattern)
          val bcHashtree = sc.broadcast(hashtree)
          frequentItemsets = transactions.flatMap(transaction => {
            val t = transaction.split(splitterPattern).sortWith(_.toInt < _.toInt)
            val itemsets = traverseHashtree(t, bcHashtree.value, splitterPattern, new ArrayBuffer[String](), k)
            itemsets.map(item => (item, 1))
          }).reduceByKey(_ + _).filter(_._2 >= minSupport).cache()
        }
        case 0 => {
          val bcCandidateItemsets = sc.broadcast(candidateItemsets).value
          frequentItemsets = parallelGenerateFrequentItemsets(sc, transactions, bcCandidateItemsets, minSupport, splitterPattern).cache()
        }
        case _ => {

        }
      }
      frequentItemsets.saveAsTextFile(outputPath + k + "-itemsets/");
      count = count + frequentItemsets.count().toInt
    }
    println("count = " + count)
  }
}

object Apriori {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("spark://sr471:7177").setAppName("Apriori").set("spark.cores.max", "128").set("spark.executor.memory", "24G")
    val sc = new SparkContext(conf)
    val method = args(0)
    val supportThreshold = args(1).toDouble
    val fileName = args(2)
    val splitterPattern = args(3)
    val numSlices = args(4).toInt
    val optimization = args(5).toInt
    val outputPath = args(6)

    val startTime = currentTime
    val input = sc.textFile("hdfs://sr471:54311/user/clin/apriori/input/" + fileName, numSlices)
    val minSupport = (supportThreshold * input.count()).toInt
    val apriori: Apriori = new Apriori(supportThreshold, splitterPattern, optimization, numSlices);

    if (method == "sequential") {
      val transactions = input.map(line => line.split(splitterPattern)).collect()
      apriori.sequentialApriori(transactions, minSupport, outputPath)
    } else {
      apriori.parallelApriori(sc, input, minSupport, splitterPattern, optimization, outputPath)
    }
    val endTime = currentTime
    val totalTime: Double = endTime - startTime

    println("---------------------------------------------------------")
    println("This program totally took " + totalTime/1000 + " seconds.")
    println("---------------------------------------------------------")

  } //end of main
}
