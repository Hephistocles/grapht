package re.toph.hybrid_db

import scala.collection.mutable.{ListBuffer, Set}
import java.util.HashMap
import anorm.{ SQL, SqlParser }

/**
  * Created by christoph on 28/04/16.
  */


class LookaheadMultiPrefetcher(hops: Int)(implicit connection:java.sql.Connection) extends Prefetcher {

  override def get(k: Long): (GraphNode, List[GraphNode]) = {
    var edgeMap = Map[Long, (ListBuffer[Edge], Map[String, Any])]()

    val idsWeWant = Set[Long](k)

    for (i <- 0 to hops) {
      val sql = SQL(
        """SELECT points.id AS id, points.lat AS lat, points.lng AS lng,
          |       edges.id1 AS id1, edges.id2 AS id2, edges.dist AS dist
          |FROM edges
          |JOIN points
          |   ON points.id=edges.id1
          |WHERE id1 IN """.stripMargin
        + idsWeWant.toString().substring(3))
      val result = Timer.time("DB", sql())

      idsWeWant.clear()

      result.foreach(row => {

        val map = getMap(row)
          .map(a => {
            val (k, s) = a
            (k.substring(k.indexOf('.') + 1), s)
          })

        // create a new node entry if we've not seen this departure point before
        if (! edgeMap.contains(row[Long]("id1"))) {
          edgeMap += row[Long]("id1") -> (ListBuffer[Edge](), map)
        }

        edgeMap(row[Long]("id1"))._1 += Edge(row[Long]("id1"), row[Long]("id2"), map)

        if (! edgeMap.contains(row[Long]("id2"))) {
          // This destination has not been explored yet
          idsWeWant += row[Long]("id2")
        }
      })
    }

    val ns = ListBuffer[GraphNode]()
    var n : GraphNode = null
    import scala.collection.JavaConversions._
    // TODO: Actually use the "hops" parameter
    edgeMap.foreach( (x:(Long, (ListBuffer[Edge], Map[String, Any]))) => {
      val (_k, (es, map)) = x
      val _n = new GraphNode(_k, es.toList, map)
      if (k==_k) n = _n
      ns += _n
    })

    (n, ns.toList)
  }
}
