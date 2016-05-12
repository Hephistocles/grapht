package re.toph.hybrid_db

import java.sql.ResultSet

import java.util.HashMap
import anorm.{ SQL, SqlParser }

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable

/**
  * Created by christoph on 28/04/16.
  */


class LookaheadBlockPrefetcher(dist: Long)(implicit connection: java.sql.Connection) extends Prefetcher {

  override def get(k: Long): (GraphNode, List[GraphNode]) = {

    // TODO: I don't like blocking on prefetching. Can we do prefetch in a BG thread or something?
    val sql =
      SQL(
        """
          | WITH res(lat, lng) AS
          |   (SELECT lat, lng from points WHERE id={id})
          | SELECT points.id, points.lat, points.lng, edges.id1, edges.id2, edges.dist
          | FROM res, points
          | JOIN edges
          |   ON points.id=edges.id1
          | WHERE
          |   points.lat < res.lat + {dist} AND
          |   points.lat > res.lat - {dist} AND
          |   points.lng < res.lng + {dist} AND
          |   points.lng > res.lng - {dist}
        """.stripMargin)
    .on("id"->k, "dist"->dist)

    val resultSet = Timer.time("DB", sql())

    var ess = Map[Long, (ListBuffer[Edge], Map[String, Any])]()

    resultSet.foreach( row => {
      // convert the row to a map
      val map: Map[String, Any] = row.asMap

        .map(a => {
          val (k, s) = a
          (k.substring(k.indexOf('.') + 1), s)
        })

      // if this is an edge for a new node, create a blank node
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
      // actually construct nodes from the data we collected
      val _n = new GraphNode(_k, es.toList, map)
      // if this is the desired node - keep track of it!
      if (k==_k) n = _n
      ns += _n
    })

    (n, ns.toList)
  }
}