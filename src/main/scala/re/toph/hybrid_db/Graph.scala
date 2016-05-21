package re.toph.hybrid_db

/**
  * Created by christoph on 28/04/16.
  */
class Graph(p :Prefetcher) {
  var evictionCount = 0
  var hitCount = 0
  var missCount = 0
  var callCount = 0

  var nodes = new java.util.LinkedHashMap[Long, GraphNode](16, 0.75f, true) {

    val maxMem = Runtime.getRuntime().maxMemory()

    override def removeEldestEntry(eldest:java.util.Map.Entry[Long, GraphNode]) : Boolean = {
      val allocatedMemory : Long = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()

      // keep 100 MB spare!
      if (maxMem - allocatedMemory < 100 * 1024) {
        evictionCount += 1
        true
      } else false
    }
  }

  def getVertex(k:Long) = {
    callCount += 1
    val node = nodes.get(k)
    if (node == null) {
      missCount += 1
//      println(s"Had to fetch $k")
      val (n, ns) = p.get(k)
      ns foreach (n => addNode(n) )
      n
    } else {
      hitCount += 1
      node
    }
  }
  def addNode(n: GraphNode) = nodes.put(n.id, n)

//  override def toString() = { var s = ""
//
//    for ((_, n) <- nodes) {
//      s += n + "\n"
//    }
//
//    s
//  }
}
