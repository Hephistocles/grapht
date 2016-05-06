package re.toph.hybrid_db

import java.sql.ResultSet

import java.util.HashMap
import anorm.{ SQL, SqlParser }

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable

/**
  * Created by christoph on 28/04/16.
  */


class LookaheadJoinPrefetcher(hops: Int)(implicit connection: java.sql.Connection) extends Prefetcher {

  override def get(k: Long): (GraphNode, List[GraphNode]) = {

    val sql =
      SQL(
        """(SELECT points.id, lat, lng, edges.id1, edges.id2, edges.dist FROM edges JOIN points ON points.id=edges.id1 WHERE id1=""" + k + """) UNION (SELECT points.id, lat, lng, B.id1, B.id2, B.dist FROM edges A JOIN edges B ON A.id2=B.id1 JOIN points ON points.id=B.id1 WHERE A.id1=""" + k + """)""")
    // TODO: Actually use the "hops" parameter
    val resultSet = Timer.time("DB", sql())
    // TODO: I don't like blocking on prefetching. Can we do prefetch in a BG thread or something?

    var ess = Map[Long, (ListBuffer[Edge], Map[String, Any])]()

    resultSet.foreach( row => {
      val map: Map[String, Any] = row.asMap.map(a => {
        val (k,v) = a
        (k.substring(1), v)
      })
      if (! ess.contains(row[Long]("id1"))) {
        ess += row[Long]("id1") -> (ListBuffer[Edge](), map)
      }

      ess(row[Long]("id1"))._1 += Edge(row[Long]("id1"), row[Long]("id2"), map)
    })

//    while (resultSet.next()) {
//
//      // create a hashmap - this contains both node and edge data, but context will ignore irrelevants
//      val map = getMap(resultSet)
//
//      // create un-edged initial node if necessary
//      if (! ess.containsKey(resultSet.getLong("id1"))) {
//        ess.put(resultSet.getLong("id1"), (ListBuffer[Edge](), map))
//      }
//
//      ess.get(resultSet.getLong("id1"))._1 += Edge(resultSet.getLong("id1"), resultSet.getLong("id2"), map)
//    }

    val ns = ListBuffer[GraphNode]()
    var n : GraphNode = null
    import scala.collection.JavaConversions._
    ess.foreach( (x:(Long, (ListBuffer[Edge], Map[String, Any]))) => {
      val (_k, (es, map)) = x
      val _n = new GraphNode(_k, es.toList, map)
      if (k==_k) n = _n
      ns += _n
    })

    (n, ns.toList)
  }
}
