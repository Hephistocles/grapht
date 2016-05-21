package re.toph.hybrid_db

import anorm.SQL

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 28/04/16.
  */


class LookaheadCTEPrefetcher(hops: Int = 3)(implicit connection: java.sql.Connection) extends Prefetcher {

  override def innerGet(k: Long): (GraphNode, List[GraphNode]) = {

    val sql = SQL(
      """
        |WITH RECURSIVE ans (id1, id2, dist, lat, lng, hops) AS
        |   (SELECT {id}::bigint, {id}::bigint, 0::bigint, lat, lng, 0 FROM points WHERE id={id}
        |
        |   UNION ALL
        |
        |   SELECT ans.id2, edges.id2, edges.dist, points.lat, points.lng, ans.hops + 1 FROM
        |     ans JOIN
        |     edges ON ans.id2=edges.id1 JOIN
        |     points ON ans.id2=points.id
        |     WHERE ans.hops < {hops}
        |   )
        |SELECT id1, id2, dist, id1 as id, lat, lng FROM ans WHERE hops > 0;
      """.stripMargin)
      .on("id"->k, "hops"->hops)

    val resultSet = Timer.time("DB", sql())

    var ess = Map[Long, (ListBuffer[Edge], Map[String, Any])]()

    resultSet.foreach( row => {
      // convert the row to a map
      val map = getMap(row)
        .map(a => {
          val (k, s) = a
          (k.substring(k.indexOf('.') + 1), s)
        })

      // if this is an edge from a new node, create a blank node
      if (! ess.contains(row[Long]("id1"))) {
        ess += row[Long]("id1") -> (ListBuffer[Edge](), map)
      }

      // add the edge to the given node
      ess(row[Long]("id1"))._1 += Edge(row[Long]("id1"), row[Long]("id2"), map)
    })

    val ns = ListBuffer[GraphNode]()
    var n : GraphNode = null
    ess.foreach( (x:(Long, (ListBuffer[Edge], Map[String, Any]))) => {
      val (_k, (es, map)) = x
      val _n = new GraphNode(_k, es.toList, map)
      if (k==_k) n = _n
      ns += _n
    })

    (n, ns.toList)
  }
}
