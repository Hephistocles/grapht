package re.toph.hybrid_db

import java.sql.ResultSet

import scala.collection.mutable._
import java.util.HashMap

/**
  * Created by christoph on 28/04/16.
  */


class LookaheadMultiPrefetcher(hops: Int) extends Prefetcher {

  override def get(k: Int): (GraphNode, List[GraphNode]) = {
    val edgeMap: HashMap[Int, (ListBuffer[Edge], HashMap[String, Object])] = new HashMap[Int, (ListBuffer[Edge], HashMap[String, Object])]()

    val idsWeWant = Set[Int](k)

    for (i <- 0 to hops) {
      val statement = connection.createStatement()
      val result = Timer.time("DB",
        {
          statement.executeQuery("SELECT * FROM edges JOIN points ON points.id=edges.id1 WHERE id1 IN "
          + idsWeWant.toString().substring(3))
        })
      idsWeWant.clear()

      while (result.next()) {

        val map = getMap(result)

        // create a new node entry if we've not seen this departure point before
        if (! edgeMap.containsKey(result.getInt("id1"))) {
          edgeMap.put(result.getInt("id1"), (ListBuffer[Edge](), map))
        }

        edgeMap.get(result.getInt("id1"))._1 += Edge(result.getInt("id1"), result.getInt("id2"), map)

        if (! edgeMap.containsKey(result.getInt("id2"))) {
          // This destination has not been explored yet
          idsWeWant += result.getInt("id2")
        }
      }
    }

    val ns = ListBuffer[GraphNode]()
    var n : GraphNode = null
    import scala.collection.JavaConversions._
    edgeMap.foreach( (x:(Int, (ListBuffer[Edge], HashMap[String, Object]))) => {
      val (_k, (es, map)) = x
      val _n = new GraphNode(_k, es.toList, map)
      if (k==_k) n = _n
      ns += _n
    })

    (n, ns.toList)
  }
}
