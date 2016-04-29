package re.toph.hybrid_db

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 28/04/16.
  */


object NullPrefetcher extends Prefetcher {

  override def get(k: Int): (GraphNode, List[GraphNode]) = {
    val statement = connection.createStatement()
    val resultSet = Timer.time("DB", {statement.executeQuery("SELECT * FROM edges JOIN points ON points.id=edges.id1 WHERE id1=" + k)})

    // TODO: Actually obtain node/edge data too
    var edges = ListBuffer[Edge]()
    var rs = resultSet
    while (resultSet.next()) {
      rs  = resultSet
      edges += Edge(resultSet.getInt("id1"), resultSet.getInt("id2"), getMap(resultSet))
    }
    val n = new GraphNode(k, edges.toList, getMap(rs))
    (n, List(n))
  }
}
