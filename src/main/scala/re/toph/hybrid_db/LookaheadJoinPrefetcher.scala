package re.toph.hybrid_db

import anorm.SQL

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 28/04/16.
  */


class LookaheadJoinPrefetcher(hops: Int = 3)(implicit connection: java.sql.Connection) extends Prefetcher {

  override def innerGet(k: Long): (GraphNode, List[GraphNode]) = {

    // TODO: I don't like blocking on prefetching. Can we do prefetch in a BG thread or something?
    val sqlBits = List(
      """(SELECT points.id, lat, lng, 0,0,0 FROM
        |  points
        |  WHERE points.id={id})""".stripMargin,
      """(SELECT points.id, lat, lng, A.id1, A.id2, A.dist FROM
         |edges A
         |  JOIN points ON points.id=A.id1
         |  WHERE A.id={id})""".stripMargin,
      """
        |(SELECT points.id, lat, lng, B.id1, B.id2, B.dist FROM
        | edges A
        |   JOIN edges B ON A.id2=B.id1
        |   JOIN points ON points.id=B.id1
        |   WHERE A.id1={id})""".stripMargin,
      """
        |(SELECT points.id, lat, lng, C.id1, C.id2, C.dist FROM
        | edges A
        |   JOIN edges B ON A.id2=B.id1
        |   JOIN edges C ON B.id2=C.id1
        |   JOIN points ON points.id=C.id1
        |   WHERE A.id1={id})""".stripMargin,
      """
        |(SELECT points.id, lat, lng, D.id1, D.id2, D.dist FROM
        | edges A
        |   JOIN edges B ON A.id2=B.id1
        |   JOIN edges C ON B.id2=C.id1
        |   JOIN edges D ON C.id2=D.id1
        |   JOIN points ON points.id=D.id1
        |   WHERE A.id1={id})""".stripMargin,
      """
      |(SELECT points.id, lat, lng, E.id1, E.id2, E.dist FROM
      | edges A
      |   JOIN edges B ON A.id2=B.id1
      |   JOIN edges C ON B.id2=C.id1
      |   JOIN edges D ON C.id2=D.id1
      |   JOIN edges E ON D.id2=E.id1
      |   JOIN points ON points.id=E.id1
      |   WHERE A.id1={id})""".stripMargin
    )
    if (hops>sqlBits.length) throw new Error(s"Can't do that many prefetch hops: $hops > ${sqlBits.length}")

    val sql =
      SQL(sqlBits.take(hops).mkString(" UNION "))
    .on("id"->k)
    val resultSet = Timer.time("DB", sql())

    var ess = Map[Long, (ListBuffer[Edge], Map[String, Any])]()

    resultSet.foreach( row => {
      // convert the row to a map
      val map = getMap(row)
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
      val _n = new GraphNode(_k, es.toList, map)
      if (k==_k) n = _n
      ns += _n
    })

    (n, ns.toList)
  }
}
