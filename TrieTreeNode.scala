import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 * Created by clin3 on 2014/12/5.
 */
class TrieTreeNode(var children: HashMap[String, TrieTreeNode] = new HashMap[String, TrieTreeNode](),
                    var isLeafNode: Boolean = false,
                    var itemsets: ArrayBuffer[String] = new ArrayBuffer[String]())extends Serializable{
}
