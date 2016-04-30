package re.toph.hybrid_db

import java.sql.{ResultSet}
import java.util.{Date}
import java.util


import re.toph.hybrid_db.accumulator._

import scala.collection.immutable.HashMap
import scala.collection.parallel.immutable

//import re.toph.hybrid_db.accumulator.sum.IntSumAccumulator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Result(edgeProps: Map[String, TAccumulator], vertexProps: Map[String, TAccumulator]) {

  def mapEdges(f: TAccumulator => TAccumulator): Result = {
    new Result(edgeProps.mapValues(f), vertexProps)
  }

  def mapVertex(f: TAccumulator => TAccumulator): Result = {
    new Result(edgeProps, vertexProps.mapValues(f))
  }

  def get(p: String): Any = {
    vertexProps.get(p) match {
      case Some(x) => x.result
      case None =>
        edgeProps.get(p) match {
          case Some(x) => x.result
          case None => None
        }
    }
  }

  def result: Map[String, Any] = {
    var m = new HashMap[String, Any]()
    m ++= edgeProps.mapValues(a => a.result)
    m ++= vertexProps.mapValues(a => a.result)
    m
  }
}


/**
  * Created by christoph on 28/04/16.
  */
object HelloWorld {

  //  type Result = (Long, Long, Int, String, Long)


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
    var res = List[Map[String, Any]]()
    for (i <- 0 to 5) {
      res = Timer.time("Lookahead Multi (" + i + ")", {
        getHopDists(3, 1, new LookaheadMultiPrefetcher(i))
      })
    }
    res.foreach(println)
  }

  def acc(a: TAccumulator, p: java.util.HashMap[String, Object]): TAccumulator = {
    a.acc(p)
  }

  /*
    SELECT start, end, length, (ACC VERTICES CONCAT id " -> ") path, (ACC EDGES SUM dist) cost FROM
    PATHS OVER myGraph
    WHERE start = ?
    AND length <= 3);
  */
  def getHopDists(n: Long, id: Int, p: Prefetcher): List[Map[String, Any]] = {
    val g = new Graph(p)

    def expand(sofar: Result, vertex: GraphNode): ListBuffer[Map[String, Any]] = {

      // calculate a new intermediate result, taking into account this vertex's properties
      val vertexResult = sofar.mapVertex((acc) => acc.acc(vertex.properties))

      val pathResults = ListBuffer[Map[String, Any]]()

      // TODO: generalise completion conditions
      if (vertexResult.get("start").asInstanceOf[Long] == id &&
        vertexResult.get("length").asInstanceOf[Int] <= n) {

        pathResults += vertexResult.result
      }

      // TODO: generalise stopping condition (perhaps variant on completion condition which is aware of non-decreasing amounts?)
      // if there's no point in constructing longer paths - stop!
      if (!(vertexResult.get("length").asInstanceOf[Int] + 1 <= n)) {
        return pathResults
      }

      // Otherwise let's look at neighbours
      vertex.edges.foreach(e => {
        val edgeResult = vertexResult.mapEdges(a => a.acc(e.properties))

        // explore neighbours to find more path results and merge the results in!
        // TODO: generalise this with a priority queue or something
        val neighbour = g.getVertex(e.to)
        pathResults ++= expand(edgeResult, neighbour)
      })

      pathResults
    }

    val results = new Result(
      HashMap(
        "start" -> new ConstAccumulator[Long](n),
        "end" -> new LastAccumulator("id", n),
        "dist" -> new ConstAccumulator[Long](1)
      ),
      HashMap(
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", ",", "")
      ))

    expand(results, g.getVertex(id)).toList
  }
}