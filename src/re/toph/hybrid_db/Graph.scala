package re.toph.hybrid_db

import scala.collection.mutable.HashMap

/**
  * Created by christoph on 28/04/16.
  */
class Graph(p :Prefetcher) {
  var nodes: HashMap[Int, GraphNode] = HashMap()

  def getVertex(k:Int) = {
    nodes.get(k) match {
      case Some(n) => n
      case None    => {
        val (n, ns) = p.get(k)
        ns foreach (n => addNode(n) )
          n
      }
                      // TODO: Actually check the DB before assuming there's nothing
    }
  }
  def addNode(n: GraphNode) = nodes += (n.id -> n)

  override def toString() = {
    var s = ""

    for ((_, n) <- nodes) {
      s += n + "\n"
    }

    s
  }
}
