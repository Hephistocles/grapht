package re.toph.hybrid_db

import java.sql.{ResultSet}
import java.util.{Date, HashMap}
import java.util


import re.toph.hybrid_db.accumulator._
//import re.toph.hybrid_db.accumulator.sum.IntSumAccumulator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer



/**
  * Created by christoph on 28/04/16.
  */
object HelloWorld {

  type Result = (Int, Int, Int, String, Int)


  def main(args: Array[String]): Unit = {

//    Timer.time("Really Null", {
//      getHopDists(5, 20, ReallyNullPrefetcher)
//    })
//    Timer.time("Null", {
//      getHopDists(5, 20, NullPrefetcher)
//    })
//    Timer.time("Lookahead Join (1)", {
//      getHopDists(5, 20, new LookaheadJoinPrefetcher(1))
//    })
    for (i <- 0 to 5) {
      Timer.time("Lookahead Multi (" + i + ")", {
        getHopDists(4, 20, new LookaheadMultiPrefetcher(i))
      })
    }
  }

  /*
    SELECT start, end, length, (ACC VERTICES CONCAT id " -> ") path, (ACC EDGES SUM dist) cost FROM
    PATHS OVER myGraph
    WHERE start = ?
    AND length <= 3);
  */
  def getHopDists(n:Int, id: Int, p:Prefetcher): List[Result] = {
    val g = new Graph(p)

    val accs = (
//      new ConstAccumulator(),
//      new LastAccumulator("id"),
//      new CountAccumulator(),
      new ConcatAccumulator("id2", ",")
//      new IntSumAccumulator("dist")
    )

    val res:ResultSet = null

    def expand(sofar:Result, vertex:GraphNode) : ListBuffer[Result] = {

//     calculate a new intermediate result
//     Note that length and dist were updated elsewhere as edge properties
      val intRes : Result = (sofar._1, vertex.id, sofar._3, sofar._4 + " -> " + vertex.id, sofar._5)

      val res = ListBuffer[Result]()
      if (intRes._1 == 1 && intRes._3 <= n) {
//      This is a path matching the desired criteria
        res += intRes
      }

      if (! (intRes._3+1 <= n) ) {
//      if there's no point in constructing longer paths - stop!
        return res
      }

//    Otherwise let's look at neighbours

      vertex.edges.foreach( e => {
        val neighbour = g.getVertex(e.to)
        val edgeRes : Result = (intRes._1, intRes._2, intRes._3 +1, intRes._4, intRes._5)
        // stick with DFS for now:
        // TODO: generalise this with a priority queue or something
        res ++= expand(edgeRes, neighbour)
      })


      res
    }

    val init : Result = (1, 0, 0, "", 0)

    val list = expand(init, g.getVertex(id)).toList
    list
  }
}