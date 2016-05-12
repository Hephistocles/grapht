package re.toph.hybrid_db

/**
  * Created by christoph on 28/04/16.
  */
class Graph(p :Prefetcher) {
  var nodes = new java.util.HashMap[Long, GraphNode]()

  def getVertex(k:Long) = {
    val node = nodes.get(k)
    if (node == null) {
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
