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

  def mapEdges(f:TAccumulator=>TAccumulator): Result = {
    new Result(edgeProps.mapValues(f), vertexProps)
  }

  def mapVertex(f:TAccumulator=>TAccumulator): Result = {
    new Result(edgeProps, vertexProps.mapValues(f))
  }

  def get(p:String) : Option[Any] = {
    vertexProps.get(p) match {
      case Some(x) => Some(x.result)
      case None =>
        edgeProps.get(p) match {
          case Some(x) => Some(x.result)
          case None => None
        }
    }
  }

  def result : Map[String, Any] = {
    var m = new HashMap[String,Any]()
    m ++= edgeProps.mapValues(a=>a.result)
    m ++= vertexProps.mapValues(a=>a.result)
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
    var res = List[Map[String,Any]]()
    for (i <- 0 to 5) {
      res = Timer.time("Lookahead Multi (" + i + ")", {
        getHopDists(3, 1, new LookaheadMultiPrefetcher(i))
      })
    }
    res.foreach(println)
  }

  def acc(a:TAccumulator, p:java.util.HashMap[String, Object]): TAccumulator = {
    a.acc(p)
  }

  /*
    SELECT start, end, length, (ACC VERTICES CONCAT id " -> ") path, (ACC EDGES SUM dist) cost FROM
    PATHS OVER myGraph
    WHERE start = ?
    AND length <= 3);
  */
  def getHopDists(n:Long, id: Int, p:Prefetcher): List[Map[String,Any]] = {
    val g = new Graph(p)

    val accs:Result = new Result(
      HashMap(
        "start" -> new ConstAccumulator[Long](n),
        "end" -> new LastAccumulator("id", n),
        "dist" -> new ConstAccumulator[Long](1)
      ),
      HashMap(
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", ",", "")
      ))
//    val acc = new ConcatAccumulator("id2", ",")
//    val accs = (
//      new TAccumulator[Long] {
//        override def acc: (Long, util.HashMap[String, Object]) => Long =
//          (acc, n) => acc
//      },
//      new TAccumulator[Long] {
//        override def acc: (Long, util.HashMap[String, Object]) => Long =
//          (acc, n) => Long2long(n.get("id").asInstanceOf[Long])
//      },
//      new TAccumulator[Int] {
//        override def acc: (Int, util.HashMap[String, Object]) => Int =
//          (acc, n) => acc + 1
//      },
//      new TAccumulator[String] {
//        override def acc: (String, util.HashMap[String, Object]) => String =
//          (acc, n) => if (acc.length() < 1) {
//            n.get("id").toString()
//          } else {
//            acc + "->" + n.get("id")
//          }
//      },
//      new TAccumulator[Long] {
//        override def acc: (Long, util.HashMap[String, Object]) => Long =
//          (acc, n) => acc + Long2long(n.get("dist").asInstanceOf[Long])
//      }
//    )


    def expand(sofar:Result, vertex:GraphNode) : ListBuffer[Map[String, Any]] = {

//     calculate a new intermediate result
//     Note that length and dist were updated elsewhere as edge properties

        val intRes = sofar.mapVertex((acc) => acc.acc(vertex.properties))
//      val intRes : Result = (
//        acc(accs._1.asInstanceOf[TAccumulator[Long]], sofar._1.asInstanceOf[Long], vertex.properties),
//        acc(accs._2, sofar._2, vertex.properties),
//        sofar._3,
//        acc(accs._4, sofar._4, vertex.properties),
//        sofar._5
//      )

      val res = ListBuffer[Map[String, Any]]()

      // TODO: refactor this mess!
      intRes.get("start") match {
        case Some(start) => if (start == id) {
          intRes.get("length") match {
            case Some(length) => if (length.asInstanceOf[Int] <= n) {
              res += intRes.result
            }
            case None =>
          }
        }
        case None =>
      }
//      if (intRes.get("start") == 1L && intRes.get("length") <= n) {
////      This is a path matching the desired criteria
//        res += intRes
//      }

//      if (! (intRes._3+1 <= n) ) {
////      if there's no point in constructing longer paths - stop!
//        return res
//      }
      intRes.get("length") match {
        case Some(length) => if (! (length.asInstanceOf[Int]+1 <= n) ) {
          return res
        }
        case None =>
      }

//    Otherwise let's look at neighbours

      vertex.edges.foreach( e => {
        val neighbour = g.getVertex(e.to)
        val edgeRes = intRes.mapEdges(a=>a.acc(e.properties))
//        val edgeRes : Result = (
//          intRes._1,
//          intRes._2,
//          acc(accs._3, intRes._3, e.properties),
//          intRes._4,
//            acc(accs._5, intRes._5, e.properties)
//          )

        // stick with DFS for now:
        // TODO: generalise this with a priority queue or something
        res ++= expand(edgeRes, neighbour)
      })

      res
    }

//    val init : Result = (1L, 0L, 0, "", 0)

    val list = expand(accs, g.getVertex(id)).toList
    list
  }
}