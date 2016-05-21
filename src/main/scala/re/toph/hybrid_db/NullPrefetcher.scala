package re.toph.hybrid_db

import anorm.SQL
import scala.collection.mutable.ListBuffer
/**
  * Created by christoph on 28/04/16.
  */


class NullPrefetcher()(implicit connection: java.sql.Connection) extends Prefetcher {

  override def innerGet(k: Long): (GraphNode, List[GraphNode]) = {
    val sql =
      SQL("SELECT * FROM edges JOIN points ON points.id=edges.id1 WHERE id1={k}")
        .on("k"->k)

    val resultSet = Timer.time("DB", sql())

    // TODO: Actually obtain node/edge data too
    var edges = ListBuffer[Edge]()
    var map:Map[String, Any] = null
    resultSet.foreach( row => {
      map = row.asMap
        .map(a => {
          val (k, s) = a
          (k.substring(k.indexOf('.') + 1), s)
        })

      edges += Edge(row[Long]("id1"), row[Long]("id2"), map)
    })
    val n = new GraphNode(k, edges.toList, map)
    (n, List(n))
  }
}
