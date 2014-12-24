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

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._
import scala.util.control._

/**
 * Created by clin3 on 2014/11/25.
 */

class DistApriori private (
    private var supportThreshold: Double,
    private var splitterPattern: String,
    private var optimization: String,
    private var degree: Int) extends Serializable {

  def this() = this(
    DistApriori.DEFAULT_SUPPORT_THRESHOLD,
    DistApriori.DEFAULT_SPLITTER_PATTERN,
    DistApriori.OPTIMIZATION_TRIETREE,
    DistApriori.DEFAULT_DEGREE)

  /** Set the support threshold. Support threshold must be defined on interval [0, 1]. Default: 0. */
  def setSupportThreshold(supportThreshold: Double): this.type = {
    if(supportThreshold < 0 || supportThreshold > 1) {
      throw new IllegalArgumentException("Support threshold must be defined on interval [0, 1]")
    }
    this.supportThreshold = supportThreshold
    this
  }

  /** Set the splitter pattern within which we can split transactions into items. */
  def setSplitterPattern(splitterPattern: String): this.type = {
    this.splitterPattern = splitterPattern
    this
  }

  /**
   * Set the optimization of Apriori algorithm to speed up counting the support of candidate itemsets.
   * Default: trietree.
   */
  def setOptimization(optimization: String): this.type = {
    if(optimization != DistApriori.OPTIMIZATION_NAIVE
      && optimization != DistApriori.OPTIMIZATION_HASHTREE
      && optimization != DistApriori.OPTIMIZATION_TRIETREE) {
      throw new IllegalArgumentException("Invalid optimization: " + optimization)
    }
    this.optimization = optimization
    this
  }

  /**
   * Set the degree of nodes in hashtree. A larger degree may consume more memory
   * but get a better performance. Default: 4096.
   */
  def setDegree(degree: Int): this.type = {
    if(degree < 0) {
      throw new IllegalArgumentException("Degree of nodes in hashtree must a be positive number.")
    }
    this.degree = degree
    this
  }

  /** Generate frequent 1-itemsets. */
  def GenerateFrequent1Itemsets(
      data: RDD[String],
      minSupport: Int): RDD[(String, Long)] = {
    data.flatMap(record =>
      record.split(splitterPattern).map(item => (item, 1L)))
      .reduceByKey(_ + _).filter(_._2 >= minSupport)
  }

  /**
   * Generate (k + 1)-candidates itemsets by computing Cartesian product of
   * frequent k-itemsets with itself.
   */
  def AprioriGen(frequentItemsets: RDD[(String, Long)], k: Int): RDD[String] = {
    val sc = frequentItemsets.sparkContext
    val bcFrequentItemsets = sc.broadcast(frequentItemsets.collect())

    frequentItemsets.flatMap(x => bcFrequentItemsets.value.flatMap(y => {
      var i = 0
      var pos = Int.MinValue
      var candidateItemset = new String()
      val items = x._1.split(splitterPattern)
      val bcItems = y._1.split(splitterPattern)

      val loop = new Breaks()
      loop.breakable(
        while(i < k) {
          if(items(i) != bcItems(i)) {
            pos = i
            loop.break()
          }
          i = i + 1
        }
      )

      if(pos == k - 1 && items(pos).toInt < bcItems(pos).toInt) {
        candidateItemset = items.slice(0, pos + 1).mkString(splitterPattern)
        candidateItemset += splitterPattern
        candidateItemset += bcItems(pos)
        Some(candidateItemset)
      } else {
        None
      }
    }))
  }

  /** Build trietree with candidate itemsets. */
  def buildTrietree(candidateItemsets: Array[String], k: Int, splitterPattern: String): TrieTreeNode = {
    val trietreeRoot = new TrieTreeNode()
    var currentNode: Option[TrieTreeNode] = None
    var parentNode: Option[TrieTreeNode] = None

    for(currentItemset <- candidateItemsets) {
      // Insert items of candidate itemsets into hashtree.
      parentNode = None
      currentNode = Some(trietreeRoot)
      for(item <- currentItemset.split(splitterPattern)) {
        val children = currentNode.get.children
        parentNode = currentNode
        // If children of current node contains this item, get the child node of current node.
        // Else initialize a new node as the child node of current node.
        if(children.contains(item)) {
          currentNode = children.get(item)
        } else {
          currentNode = Some(new TrieTreeNode())
          children.put(item, currentNode.get)
        }
        parentNode.get.children = children
      }
      // Set current node as leaf node and insert current itemset into it.
      currentNode.get.isLeafNode = true
      val itemsets = currentNode.get.itemsets
      currentNode.get.itemsets = (itemsets += currentItemset)
    }

    trietreeRoot
  }

  /** Traverse trietree and find all candidate itemsets in current record. */
  def traverseTrietree(
      trietreeRoot: TrieTreeNode,
      record: String,
      startIndex: Int): ArrayBuffer[String] = {
    if(trietreeRoot.isLeafNode == true) {
      trietreeRoot.itemsets
    } else {
      val retVals = new ArrayBuffer[String]()
      val items = record.split(splitterPattern)
      for(i <- startIndex until items.length) {
        val item = items(i)
        val children = trietreeRoot.children
        if(children.contains(item)) {
          retVals ++= traverseTrietree(children.get(item).get, record, i + 1)
        }
      }
      retVals
    }
  }

  def hash(key: Int): Int = {
    key % degree
  }

  /** Build hashtree with canidate itemsets. */
  def buildHashtree(candidateItemsets: Array[String], k: Int): HashTreeNode = {
    def findNextNode(node: HashTreeNode, items: Array[String]): HashTreeNode = {
      val h = hash(items(node.level).toInt)
      node.children(h)
    }

    def leafToBranch(node: HashTreeNode): Unit = {
      for(i <- 0 until degree) {
        node.children(i) = new HashTreeNode(new Array[HashTreeNode](degree))
        node.children(i).level = node.level + 1
      }
      node.isLeafNode = false
      for(currentItemset <- node.candidateItemsets) {
        val items = currentItemset.split(splitterPattern)
        val h = hash(items(node.level).toInt)
        val child = node.children(h)
        insertItemsets(child, currentItemset)
      }
      node.candidateItemsets = new ArrayBuffer[String]()
    }

    def insertItemsets(currentNode: HashTreeNode, items: String): Unit = {
      currentNode.candidateItemsets += items
    }

    val hashtreeRoot = new HashTreeNode(new Array[HashTreeNode](degree))
    for(currentItemset <- candidateItemsets) {
      val items = currentItemset.split(splitterPattern)
      var currentNode = hashtreeRoot
      // If current node is not leaf node, find the child node of current node according to hash value.
      while(currentNode.isLeafNode == false) {
        currentNode = findNextNode(currentNode, items)
      }
      // If there are more than degree candidate itemsets in current node,
      // we compute hash values of itemsets in current node, initilize children nodes of it,
      // and transfer itemsets to children nodes of it. Then we mark current node as not a leaf node.
      while(currentNode.candidateItemsets.length >= degree && currentNode.level < k) {
        leafToBranch(currentNode)
        currentNode = findNextNode(currentNode, items)
      }
      insertItemsets(currentNode, currentItemset)
    }
    hashtreeRoot
  }

  /** Traverse hashtree and find all candidate itemsets in current record. */
  def traverseHashtree(
      record: Array[String],
      node: HashTreeNode,
      prefix: ArrayBuffer[String],
      k: Int): ArrayBuffer[String] = {
    def isSubset(
        currentItemset: String,
        record: Array[String],
        prefix: ArrayBuffer[String]): Boolean = {
      val items = currentItemset.split(splitterPattern)
      var retVal = true
      val loop = new Breaks()

      loop.breakable (
        for (i <- 0 until items.length) {
          val item = items(i)
          if (i < prefix.length) {
            if (item != prefix(i)) {
              retVal = false
              loop.break()
            }
          } else {
            if (!record.contains(item)) {
              retVal = false
              loop.break()
            }
          }
        }
      )

      retVal
    }

    val retVals = new ArrayBuffer[String]()
    // If current node is a leaf node, we collect those candidate itemsets in this node
    // which are subsets of current record.
    // Else we recursively find all candidate itemsets in sub-record.
    if (node.isLeafNode == true) {
      for (currentItemset <- node.candidateItemsets) {
        if (isSubset(currentItemset, record, prefix) == true) {
          retVals.append(currentItemset)
        }
      }
    } else {
      if (prefix.length < k && record.length >= 1) {
        val subRecord = record.slice(1, record.length)
        val q = node.children(hash(record(0).toInt))
        retVals ++= traverseHashtree(subRecord, q, prefix :+ record(0), k)
        retVals ++= traverseHashtree(subRecord, node, prefix, k)
      }
    }
    retVals
  }

  /**
   * Generate frequent k-itemsets. We broadcast candidate itemsets to workers and
   * count support of candidate itemset on each worker. Then we reduce all results and
   * filter out itemsets with support no less than minimum support.
   */
  def GenerateFrequentItemsets (
      data: RDD[String],
      bcCandidateItemsets: Array[String],
      minSupport: Int): RDD[(String, Long)] = {
    data.flatMap(record => {
      val items = record.split(splitterPattern)
      for(candidateItemset <- bcCandidateItemsets) yield {
        var mismatch = false
        val splittedCandidateItemset = candidateItemset.split(splitterPattern)

        val loop = new Breaks()
        loop.breakable(
          for(item <- splittedCandidateItemset) {
            // If a item of candidate itemset do not appear in data, set mismatch true and break.
            if(!items.contains(item)) {
              mismatch = true
              loop.break()
            }
          }
        )

        if(mismatch) {
          (splittedCandidateItemset.mkString(splitterPattern), 0L)
        } else {
          (splittedCandidateItemset.mkString(splitterPattern), 1L)
        }
      }
    }).reduceByKey(_ + _).filter(_._2 >= minSupport)
  }

  /**
   * Compute the minimum support according to support the threshold.
   * Sort records of data in ascending order.
   */
  def run(data: RDD[String]): RDD[(String, Long)] = {
    val minSupport = (supportThreshold * data.count()).toInt
    val sortedData = data.map(record =>
      record.split(splitterPattern).sortWith(_.toInt < _.toInt).mkString(splitterPattern)
    )
    run(sortedData, minSupport)
  }

  /** Implementation of DistApriori. */
  def run(data: RDD[String], minSupport: Int): RDD[(String, Long)] = {
    var k = 1
    val sc = data.sparkContext
    var frequentItemsets = GenerateFrequent1Itemsets(data, minSupport).persist()
    var retRDD = frequentItemsets

    while(frequentItemsets.count() != 0) {
      k = k + 1
      val candidateItemsets = AprioriGen(frequentItemsets, k - 1).collect()

      optimization match {
        case DistApriori.OPTIMIZATION_TRIETREE => {
          val trietree = buildTrietree(candidateItemsets, k, splitterPattern)
          val bcTrietree = sc.broadcast(trietree)

          frequentItemsets = data.flatMap(record => {
            traverseTrietree(bcTrietree.value, record, 0)
            .map(item => (item, 1L))
          }).reduceByKey(_ + _).filter(_._2 >= minSupport).persist()
        }

        case DistApriori.OPTIMIZATION_HASHTREE => {
          val hashtree = buildHashtree(candidateItemsets, k)
          val bcHashtree = sc.broadcast(hashtree)

          frequentItemsets = data.flatMap(record => {
            val items = record.split(splitterPattern)
            traverseHashtree(items, bcHashtree.value, new ArrayBuffer[String](), k)
            .map(item => (item, 1L))
          }).reduceByKey(_ + _).filter(_._2 >= minSupport).persist()
        }

        case DistApriori.OPTIMIZATION_NAIVE => {
          val bcCandidateItemsets = sc.broadcast(candidateItemsets)
          frequentItemsets =
            GenerateFrequentItemsets(data, bcCandidateItemsets.value, minSupport).persist()
        }

      }
      retRDD = retRDD.++(frequentItemsets)
    }
    retRDD
  }
}

/**
 * Top-level methods for calling DistApriori.
 */
object DistApriori {

  // Optimization method names and default values.
  val OPTIMIZATION_NAIVE = "naive"
  val OPTIMIZATION_TRIETREE = "trietree"
  val OPTIMIZATION_HASHTREE = "hashtree"
  val DEFAULT_SPLITTER_PATTERN = " "
  val DEFAULT_SUPPORT_THRESHOLD = 0
  val DEFAULT_DEGREE = 4096

  /**
   * Run DistApriori using the given set of parameters.
   * @param data transactional datasets stored as `RDD[String]`
   * @param supportThreshold support threshold
   * @param splitterPattern splitter pattern
   * @param optimization optimization method
   * @param degree the degree of a node in hashtree, only works in optimization: hashtree
   * @return frequent itemsets stored as `RDD[String]`
   */
  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String,
      optimization: String,
      degree: Int): RDD[(String, Long)] = {
    new DistApriori().setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .setOptimization(optimization)
      .setDegree(degree)
      .run(data)
  }

  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String,
      optimization: String): RDD[(String, Long)] = {
    new DistApriori().setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .setOptimization(optimization)
      .run(data)
  }

  def run(
           data: RDD[String],
           supportThreshold: Double,
           splitterPattern: String): RDD[(String, Long)] = {
    new DistApriori().setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .run(data)
  }

  def run(data: RDD[String], supportThreshold: Double): RDD[(String, Long)] = {
    new DistApriori().setSupportThreshold(supportThreshold)
      .run(data)
  }

  def run(data: RDD[String]): RDD[(String, Long)] = {
    new DistApriori().run(data)
  }
}
