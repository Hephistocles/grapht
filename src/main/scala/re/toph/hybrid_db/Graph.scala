package re.toph.hybrid_db

/**
  * Created by christoph on 28/04/16.
  */
class Graph(p :Prefetcher) {
  var nodes = new java.util.LinkedHashMap[Long, GraphNode](16, 0.75f, true) {

    val maxMem = Runtime.getRuntime().maxMemory()

    override def removeEldestEntry(eldest:java.util.Map.Entry[Long, GraphNode]) : Boolean = {
      val allocatedMemory : Long = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()

      // keep 100 MB spare!
      maxMem - allocatedMemory < 100 * 1024
    }
  }

  def getVertex(k:Long) = {
    val node = nodes.get(k)
    if (node == null) {
//      println(s"Had to fetch $k")
      val (n, ns) = p.get(k)
      ns foreach (n => addNode(n) )
      n
    } else {
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
