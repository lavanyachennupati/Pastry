import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.Random
import java.security.MessageDigest
import java.math.BigInteger

object project3 extends App {
  (new PastryHandler(args(0).toInt, args(1).toInt)).start()

trait Message extends Serializable
case class RoutingMessages(msg: String, nodeToRoute: PastryNode, hashmap: HashMap[String, PastryNode], noOfHops: Int) extends Message
case class AddToNetwork(hashmap: HashMap[String, PastryNode]) extends Message
case class AddAcks(node: PastryNode) extends Message
case class UpdateStateInfo(node: PastryNode, hashmap: HashMap[String, PastryNode], isFromDestinationNode: Boolean) extends Message
case class UpdateNodesValues(node: PastryNode, hashmap: HashMap[String, PastryNode]) extends Message
case class UpdateAck(node: PastryNode) extends Message
case class StartRequests(msg: String, numofRequests: Int,
  destinationslist: ArrayBuffer[PastryNode], hashmap: HashMap[String, PastryNode]) extends Message
case class DeliverMsg(msg: String, destination: PastryNode, var noOfHops: Int) extends Message
case class Terminate() extends Message

object Constants {
  val ADD: String = "add"
  val REQUEST: String = "pastry"
}
 
object NodeIdFactory {

  var nextnode: Long = (new Random(System.currentTimeMillis())).nextLong()

  def generateRandomNodeId(): NodeId = {
    
    nextnode += 1

    val original: Array[Byte] = new Array[Byte](8)
    var temp: Long = nextnode 
    for (i <- 0 to 7) {
      original(i) = (temp & 0xff).toByte
      temp >>= 8
    }

    val md: MessageDigest = MessageDigest.getInstance("SHA1")
    md.update(original)
    val digest: Array[Byte] = md.digest()

    val nid: NodeId = new NodeId()
    nid.initNodeId(digest)
    return nid
  }
}


class NodeId {
  
  val bitLength: Int = 128
  var originalId: Array[Byte] = null
  var stringId: String = null

  def initNodeId(id: Array[Byte]) = {
    val numBytes: Int = bitLength >> 3

    originalId = new Array[Byte](numBytes)

    for (i: Int <- 0 until numBytes)
      originalId(i) = id(i)

    stringId = convertToHex(originalId)
  }

  override def toString(): String = {
    return stringId
  }

  private def convertToHex(original: Array[Byte]): String = {
    var buffer: StringBuffer = new StringBuffer();
    for (i <- 0 until original.length) {
      var halfbyte: Int = (original(i) >>> 4) & 0x0F;
      var two_halfs: Int = 0;
      while (two_halfs < 2) {
        if ((0 <= halfbyte) && (halfbyte <= 9))
          buffer.append(('0' + halfbyte).toChar);
        else
          buffer.append(('a' + (halfbyte - 10)).toChar);
        halfbyte = original(i) & 0x0F;
        two_halfs += 1
      }
    }
    return buffer.toString();
  }
} 


class NeighborhoodSetNodes {
  val size: Int = 16 
  var neighbors: ArrayBuffer[(Int, String)] = new ArrayBuffer[(Int, String)](size)
}
class LeafSetNodes(val presentNodeId: String) {
  val size: Int = 8 
  val initSmallerSetSize = size / 2
  val initLargerSetSize = size - initSmallerSetSize
  var smallerSet: ArrayBuffer[String] = new ArrayBuffer[String](initSmallerSetSize)
  var largerSet: ArrayBuffer[String] = new ArrayBuffer[String](initLargerSetSize)
}
class RoutingTableInfo(presentNodeId: NodeId) {
  val rows: Int = 32 
  val cols: Int = 16
  var table: Array[Array[String]] = Array.tabulate[String](rows, cols)((x, y) => null)
}



class PastryHandler(val numofNodes: Int, val numofRequests: Int) extends Actor {
  
  val nodes: ArrayBuffer[PastryNode] = new ArrayBuffer[PastryNode]()
  val hashmap: HashMap[String, PastryNode] = new HashMap[String, PastryNode]

  def act() {
    for (i <- 0 until numofNodes) { // for (node <- nodes)
      val nodeIdValue: NodeId = NodeIdFactory.generateRandomNodeId()
      val node: PastryNode = new PastryNode(nodeIdValue, i, this)
      node.linktoNeighbor(getNearestNode(node))
      nodes.append(node)
    }

    var indexnumnum: Int = 0
    // Bootstrap node
    nodes(indexnumnum).start()
    nodes(indexnumnum) ! AddToNetwork(hashmap)

    var numofMsgDelivered: Int = 0
    var totalnumofHops: Int = 0

    loop {
      react {
        case AddAcks(node: PastryNode) =>
         
          hashmap += node.id.stringId -> node
         
          if (indexnumnum < numofNodes - 1) {
            indexnumnum += 1
            nodes(indexnumnum).start()
            nodes(indexnumnum) ! AddToNetwork(hashmap)
          } else { 
            for (node <- nodes) {
              node ! StartRequests(Constants.REQUEST, numofRequests, selectRandomDestination(node, nodes, numofRequests), hashmap)
            }
          }
        case DeliverMsg(msg: String, destination: PastryNode, noOfHops: Int) =>
          numofMsgDelivered += 1
          totalnumofHops += noOfHops

          if (numofMsgDelivered == (numofNodes * numofRequests)) {
           
            println("Average No Of Hops when convergence is reached is : " + totalnumofHops.toDouble / numofMsgDelivered.toDouble)
            for (node <- nodes)
              node ! Terminate()

            exit
          }
      }
    }
  }

  def selectRandomDestination(srcNode: PastryNode, liveNodesList: ArrayBuffer[PastryNode], numofRequests: Int): ArrayBuffer[PastryNode] = {
    var destinationslist: ArrayBuffer[PastryNode] = new ArrayBuffer[PastryNode](numofRequests)
    var i:Int = 0
    while (i < numofRequests) {
      var indexnumnum: Int = Random.nextInt(liveNodesList.length)
      if (srcNode.id.stringId != liveNodesList(indexnumnum).id.stringId &&
        !destinationslist.exists(node => node.id.stringId == liveNodesList(indexnumnum).id.stringId)) {
        destinationslist.append(liveNodesList(indexnumnum))
        i += 1
      }
    }
    return destinationslist
  }

  def getNearestNode(presentNode: PastryNode): PastryNode = {
    var minimumdistance: Long = Long.MaxValue
    var returnNode: PastryNode = presentNode 
    for (node <- nodes) {
      if (presentNode.id != node.id) {
        val distance = math.abs(presentNode.proxyNum - node.proxyNum)
        if (distance < minimumdistance) {
          minimumdistance = distance
          returnNode = node
        }
      }
    }
    return returnNode 
  }
}


class PastryNode(var id: NodeId, var proxyNum: Int, master: PastryHandler) extends Actor {

  def this(id: NodeId, master: PastryHandler) = this(id, -1, master)

  var LeafSetNodes: LeafSetNodes = new LeafSetNodes(this.id.stringId)
  var routingtable: RoutingTableInfo = new RoutingTableInfo(this.id)
  var NeighborhoodSetNodes: NeighborhoodSetNodes = new NeighborhoodSetNodes()

  var closestPeer: PastryNode = null
  var isReady: Boolean = false

  def linktoNeighbor(physicalNeighbor: PastryNode) = {
    if (physicalNeighbor != null && physicalNeighbor.id != this.id)
      closestPeer = physicalNeighbor
  }

  
  def shl(string1: String, string2: String): Int = {
    if (string1.length() != string2.length())
      throw new Exception("Strings of unequal length")
    var i: Int = -1
    for (i <- 0 until string1.length) {
      if (string1.charAt(i) != string2.charAt(i))
        return i
    }
    return i + 1
  }

  def getClosestFromIDnLeafSetNodes(key: String): String = {
    var LeafSetNodes: ArrayBuffer[String] = ArrayBuffer.empty
    var closestNodeId: String = this.id.stringId
    val keyId: BigInteger = new BigInteger(key, 16)
    var difference: BigInteger = (keyId.subtract(new BigInteger(this.id.stringId, 16))).abs()
    var mindifference: BigInteger = difference


    if (key == this.id.stringId)
      return key
    else if (key < this.id.stringId && this.LeafSetNodes.smallerSet.length > 0
      && key > this.LeafSetNodes.smallerSet(this.LeafSetNodes.smallerSet.length - 1)) {
      LeafSetNodes = this.LeafSetNodes.smallerSet
    } else if (key > this.id.stringId && this.LeafSetNodes.largerSet.length > 0
      && key < this.LeafSetNodes.largerSet(this.LeafSetNodes.largerSet.length - 1)) {
      LeafSetNodes = this.LeafSetNodes.largerSet
    }

    if (LeafSetNodes.length != 0) { // key falls within the range
      for (i: Int <- 0 until LeafSetNodes.length) {
        val id: BigInteger = new BigInteger(LeafSetNodes(i), 16)
        difference = (keyId.subtract(id)).abs()
        if (mindifference.compareTo(difference) > 0) {
          mindifference = difference
          closestNodeId = LeafSetNodes(i)
        }
      }
    } else { 
      val lengthOfPrefix: Int = shl(this.id.stringId, key) 
      val nextDigitValue: Int = key.charAt(lengthOfPrefix).asDigit 

      if (this.routingtable.table(lengthOfPrefix)(nextDigitValue) != null &&
        this.routingtable.table(lengthOfPrefix)(nextDigitValue) != "") {
        val item = this.routingtable.table(lengthOfPrefix)(nextDigitValue)
        difference = (keyId.subtract(new BigInteger(item, 16))).abs
        if (mindifference.compareTo(difference) > 0) {
          mindifference = difference
          closestNodeId = item
        }
      }
    }

    return closestNodeId
  }

  def getNumericallyCloserKey(key1: String, key2: String): String = {
    if (key1 == null && key2 == null) return null 
    if (key1 == null || key1 == "") return key2
    if (key2 == null || key2 == "") return key1

  
    val id: BigInteger = new BigInteger(this.id.stringId, 16)
    val id1: BigInteger = new BigInteger(key1, 16)
    val id2: BigInteger = new BigInteger(key2, 16)

    if ((id.subtract(id1).abs).compareTo(id.subtract(id2).abs) <= 0)
      return key1
    return key2
  }

  def AppendItemsInLeafSetNodes(id: String) = {
    
    if (id < this.id.stringId)
      this.LeafSetNodes.smallerSet.append(id)
    else if (id > this.id.stringId) 
      this.LeafSetNodes.largerSet.append(id)
  }

  def BalanceLeafSetNodes() {
    this.LeafSetNodes.smallerSet = this.LeafSetNodes.smallerSet.distinct
    this.LeafSetNodes.smallerSet = this.LeafSetNodes.smallerSet.sortWith((string1, string2) => string1 > string2)
    this.LeafSetNodes.largerSet = this.LeafSetNodes.largerSet.distinct
    this.LeafSetNodes.largerSet = this.LeafSetNodes.largerSet.sortWith((string1, string2) => string1 < string2)

   
    if (this.LeafSetNodes.smallerSet.length > this.LeafSetNodes.initSmallerSetSize) {
      this.LeafSetNodes.smallerSet.remove(
        this.LeafSetNodes.initSmallerSetSize, this.LeafSetNodes.smallerSet.length - this.LeafSetNodes.initSmallerSetSize)
    }
    if (this.LeafSetNodes.largerSet.length > this.LeafSetNodes.initLargerSetSize) {
      this.LeafSetNodes.largerSet.remove(
        this.LeafSetNodes.initLargerSetSize, this.LeafSetNodes.largerSet.length - this.LeafSetNodes.initLargerSetSize)
    }
  }

  def TablesUpdate(fromNodeNum: PastryNode, hashmap: HashMap[String, PastryNode]) = {
  
    AppendItemsInLeafSetNodes(fromNodeNum.id.stringId)
    for (stringId <- fromNodeNum.LeafSetNodes.smallerSet)
      AppendItemsInLeafSetNodes(stringId)
    for (stringId <- fromNodeNum.LeafSetNodes.largerSet)
      AppendItemsInLeafSetNodes(stringId)
   
    BalanceLeafSetNodes()
    val lengthOfPrefix: Int = shl(this.id.stringId, fromNodeNum.id.stringId) 
    val nextDigitValue: Int = fromNodeNum.id.stringId.charAt(lengthOfPrefix).asDigit 
    this.routingtable.table(lengthOfPrefix)(nextDigitValue) =
      getNumericallyCloserKey(this.routingtable.table(lengthOfPrefix)(nextDigitValue), fromNodeNum.id.stringId)

    for (keysnum <- fromNodeNum.routingtable.table)
      for (key <- keysnum)
        if (key != null && key != "" && key != this.id.stringId) {
          val prefixLen: Int = shl(this.id.stringId, key) 
          val nxtDigitNUm: Int = key.charAt(prefixLen).asDigit 
          this.routingtable.table(prefixLen)(nxtDigitNUm) =
            getNumericallyCloserKey(this.routingtable.table(prefixLen)(nxtDigitNUm), key)
        }
  
    this.NeighborhoodSetNodes.neighbors = fromNodeNum.NeighborhoodSetNodes.neighbors.clone()
   
  }

  def act() {
    var updateMsgsSent: Int = 0
    var updateAcksRcvd: Int = 0

    loop {
      react {
        case AddToNetwork(hashmap: HashMap[String, PastryNode]) =>
      
          if (closestPeer == null) { 
            isReady = true
            master ! AddAcks(this)
          } else {
            closestPeer ! RoutingMessages(Constants.ADD, this, hashmap, 0)
          }
        case RoutingMessages(msg: String, nodeToAdd: PastryNode, hashmap: HashMap[String, PastryNode], noOfHops: Int) =>
        
          val closestNode: String = getClosestFromIDnLeafSetNodes(nodeToAdd.id.stringId)
          if (closestNode == this.id.stringId) {
           
            if (msg == Constants.ADD)
              nodeToAdd ! UpdateStateInfo(this, hashmap, true)
            else
              master ! DeliverMsg(msg, this, noOfHops)
          } else {
            
            if (msg == Constants.ADD)
              nodeToAdd ! UpdateStateInfo(this, hashmap, false)
            var nodeToForwardMsg: PastryNode = hashmap(closestNode)
            nodeToForwardMsg ! RoutingMessages(msg, nodeToAdd, hashmap, noOfHops + 1)
          }

        case UpdateStateInfo(fromNodeNum: PastryNode, hashmap: HashMap[String, PastryNode], isFromDestinationNode: Boolean) =>

          TablesUpdate(fromNodeNum, hashmap)

          if (isFromDestinationNode)
            isReady = true
          if (isReady && this.mailboxSize == 0) {
         
            for (key <- LeafSetNodes.smallerSet.distinct) {
              updateMsgsSent += 1
              hashmap(key) ! UpdateNodesValues(this, hashmap)
            }
            for (key <- LeafSetNodes.largerSet) {
              updateMsgsSent += 1
              hashmap(key) ! UpdateNodesValues(this, hashmap)
            }

            for (keysnum <- routingtable.table)
              for (key <- keysnum)
                if (key != null && key != "" && key != this.id.stringId) {
                  updateMsgsSent += 1
                  hashmap(key) ! UpdateNodesValues(this, hashmap)
                }
          }

        case UpdateNodesValues(fromNodeNum: PastryNode, map: HashMap[String, PastryNode]) =>
          TablesUpdate(fromNodeNum, map)
          fromNodeNum ! UpdateAck(this)

        case UpdateAck(from: PastryNode) =>
          updateAcksRcvd += 1
          if (updateAcksRcvd == updateMsgsSent)
            master ! AddAcks(this)

        case StartRequests(msg: String, numofRequests: Int,
          destinationslist: ArrayBuffer[PastryNode], hashmap: HashMap[String, PastryNode]) =>
          for (dest <- destinationslist) {
            self ! RoutingMessages(msg, dest, hashmap, 0)
          
          }

        case Terminate() =>
          exit
      }
    }
  }

  def log(N: Int, base: Int): Int = {
    return (math.log(N) / math.log(base)).ceil.toInt
  }
}




}