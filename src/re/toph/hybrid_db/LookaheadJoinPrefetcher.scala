package re.toph.hybrid_db

import java.sql.ResultSet

import java.util.HashMap

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 28/04/16.
  */


class LookaheadJoinPrefetcher(hops: Int) extends Prefetcher {

  override def get(k: Int): (GraphNode, List[GraphNode]) = {
    val statement = connection.createStatement()

    // TODO: Actually use the "hops" parameter
    val resultSet = Timer.time("DB", {statement.executeQuery("(SELECT points.id, lat, lng, edges.id1, edges.id2, edges.dist FROM edges JOIN points ON points.id=edges.id1 WHERE id1=" + k + ") UNION (SELECT points.id, lat, lng, B.id1, B.id2, B.dist FROM edges A JOIN edges B ON A.id2=B.id1 JOIN points ON points.id=B.id1 WHERE A.id1=" + k + ")")} )

    // TODO: I don't like blocking on prefetching. Can we do prefetch in a BG thread or something?

    val ess:HashMap[Int, (ListBuffer[Edge], HashMap[String, String])] = new HashMap[Int, (ListBuffer[Edge], HashMap[String, String])]

    while (resultSet.next()) {

      // create a hashmap - this contains both node and edge data, but context will ignore irrelevants
      val map = getMap(resultSet)

      // create un-edged initial node if necessary
      if (! ess.containsKey(resultSet.getInt("id1"))) {
        ess.put(resultSet.getInt("id1"), (ListBuffer[Edge](), map))
      }

      ess.get(resultSet.getInt("id1"))._1 += Edge(resultSet.getInt("id1"), resultSet.getInt("id2"), map)
    }

    val ns = ListBuffer[GraphNode]()
    var n : GraphNode = null
    import scala.collection.JavaConversions._
    ess.foreach( (x:(Int, (ListBuffer[Edge], HashMap[String, String]))) => {
      val (_k, (es, map)) = x
      val _n = new GraphNode(_k, es.toList, map)
      if (k==_k) n = _n
      ns += _n
    })

    (n, ns.toList)
  }
}
