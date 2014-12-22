import scala.collection.mutable.ArrayBuffer

/**
 * Created by clin3 on 2014/12/11.
 */
class HashTreeNode(val children: Array[HashTreeNode] = new Array[HashTreeNode](4096),
                    var level: Int = 0,
                    var isLeafNode: Boolean = true,
                    var candidateItemsets: ArrayBuffer[String] = new ArrayBuffer[String]()) extends Serializable{

}
