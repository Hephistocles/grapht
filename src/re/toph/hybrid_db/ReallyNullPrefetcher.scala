package re.toph.hybrid_db

import java.util

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 28/04/16.
  */


object ReallyNullPrefetcher extends Prefetcher {

  override def get(k: Int): (GraphNode, List[GraphNode]) = {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT * FROM edges JOIN points ON points.id=edges.id1 WHERE id1=" + k)

    // TODO: Actually obtain node/edge data too
    var edges = ListBuffer[Edge]()
    var map:util.HashMap[String, Object] = null
    while (resultSet.next()) {
      map = getMap(resultSet)
      edges += Edge(resultSet.getInt("id1"), resultSet.getInt("id2"), map)
    }
    val n = new GraphNode(k, edges.toList, map)
    (n, Nil)
  }
}
