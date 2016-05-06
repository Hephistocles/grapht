package re.toph.hybrid_db

import java.util

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 28/04/16.
  */


object NullPrefetcher extends Prefetcher {

  override def get(k: Long): (GraphNode, List[GraphNode]) = {
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT * FROM edges JOIN points ON points.id=edges.id1 WHERE id1=" + k)

    // TODO: Actually obtain node/edge data too
    var edges = ListBuffer[Edge]()
    var map:Map[String, Any] = null
    while (resultSet.next()) {
      map = getMap(resultSet)
      edges += Edge(resultSet.getLong("id1"), resultSet.getLong("id2"), map)
    }
    val n = new GraphNode(k, edges.toList, map)
    (n, Nil)
  }
}
