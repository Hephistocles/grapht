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

    def expand(vertex: GraphNode, sofar: Result, condition:CompletionCondition): ListBuffer[Map[String, Any]] = {

      // calculate a new intermediate result, taking into account this vertex's properties
      val vertexResult = sofar.mapVertex((acc) => acc.acc(vertex.properties))

      val pathResults = ListBuffer[Map[String, Any]]()

      if (condition.check(vertexResult)) {
        pathResults += vertexResult.result
      }

      // if there's no point in constructing longer paths - stop!
      if (! condition.nextSatisfiable(vertexResult)) {
        return pathResults
      }

      // Otherwise let's look at neighbours
      vertex.edges.foreach(e => {
        val edgeResult = vertexResult.mapEdges(a => a.acc(e.properties))

        // explore neighbours to find more path results and merge the results in!
        // TODO: generalise this with a priority queue or something
        //       also important to avoid stackoverflows on large expansions
        val neighbour = g.getVertex(e.to)
        pathResults ++= expand(neighbour, edgeResult, condition)
      })

      pathResults
    }

    abstract class VarDirection[T]
    case class Increasing[T](by:Option[T]) extends VarDirection[T]
    case class Decreasing[T](by:Option[T]) extends VarDirection[T]
    case class Constant[T]() extends VarDirection[T]
    case class Variable[T]() extends VarDirection[T]

    trait CompletionCondition {
      def check(r:Result):Boolean
      def nextSatisfiable(r:Result):Boolean
    }

    case class OrCondition(cs:CompletionCondition*) extends CompletionCondition {
      override def check(r:Result) = ! cs.forall(c => !c.check(r))
      override def nextSatisfiable(r: Result): Boolean = ! cs.forall(c => !c.nextSatisfiable(r))
    }
    case class AndCondition(cs:CompletionCondition*) extends CompletionCondition {
      override def check(r:Result) = cs.forall(c =>c.check(r))
      override def nextSatisfiable(r: Result): Boolean = cs.forall(c => c.nextSatisfiable(r))
    }

    // NB we only have comparisons to const here - might want between two rows too!
    case class GreaterThanCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
      override def check(r: Result) = num.gt(r.get(prop).asInstanceOf[T], item)
      override def nextSatisfiable(r: Result): Boolean = d match {
        // first two cases are easy - we know how much is de/increasing by so can just check
        case Increasing(Some(x)) =>
          num.gt(num.plus(r.get(prop).asInstanceOf[T], x), item)
        case Decreasing(Some(x)) =>
          num.gt(num.minus(r.get(prop).asInstanceOf[T], x), item)
        case Constant() => check(r)
        case _ => // lacking any further information, assume it might be satisfied next time
          true
      }
    }

    case class GreaterThanOrEqualCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
      override def check(r: Result) = num.gteq(r.get(prop).asInstanceOf[T], item)
      override def nextSatisfiable(r: Result): Boolean = d match {
        // first two cases are easy - we know how much is de/increasing by so can just check
        case Increasing(Some(x)) =>
          num.gteq(num.plus(r.get(prop).asInstanceOf[T], x), item)
        case Decreasing(Some(x)) =>
          num.gteq(num.minus(r.get(prop).asInstanceOf[T], x), item)
        // if I'm equal and I decrease at all, next turn is invalid (so check I am currently precisely greater)
        case Decreasing(None) =>
          num.gt(r.get(prop).asInstanceOf[T], item)
        case Constant() => check(r)
        case _ => // lacking any further information, assume it might be satisfied next time
          true
      }
    }

    case class LessThanCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
      override def check(r: Result) = num.lt(r.get(prop).asInstanceOf[T], item)
      override def nextSatisfiable(r: Result): Boolean = d match {
        // first two cases are easy - we know how much is de/increasing by so can just check
        case Increasing(Some(x)) =>
          num.lt(num.plus(r.get(prop).asInstanceOf[T], x), item)
        case Decreasing(Some(x)) =>
          num.lt(num.minus(r.get(prop).asInstanceOf[T], x), item)
        case Constant() => check(r)
        case _ => // lacking any further information, assume it might be satisfied next time
          true
      }
    }

    case class LessThanOrEqualCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
      override def check(r: Result) = num.lteq(r.get(prop).asInstanceOf[T], item)
      override def nextSatisfiable(r: Result): Boolean = d match {
        // first two cases are easy - we know how much is de/increasing by so can just check
        case Increasing(Some(x)) =>
          num.lteq(num.plus(r.get(prop).asInstanceOf[T], x), item)
        case Decreasing(Some(x)) =>
          num.lteq(num.minus(r.get(prop).asInstanceOf[T], x), item)
        // if I'm equal and I increase at all, next turn is invalid (so check I am currently precisely lesser)
        case Increasing(None) =>
          num.lt(r.get(prop).asInstanceOf[T], item)
        case Constant() => check(r)
        case _ => // lacking any further information, assume it might be satisfied next time
          true
      }
    }
    case class EqualityCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
      override def check(r: Result) = {
        val v = r.get(prop)
        num.equiv(v.asInstanceOf[T], item)
      }
      override def nextSatisfiable(r:Result) : Boolean = d match {
        // if we're moving, next turn can only be possible if we're not currently equal
        case Increasing(_) | Decreasing(_) =>
          ! num.eq(r.get(prop).asInstanceOf[T], item)
        case Constant() => check(r)
        case _ => true
      }
    }

    val results = new Result(
      HashMap(
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", "->", "(" + id + ")")
      ),
      HashMap(
        "start" -> new ConstAccumulator[Long](id),
        "end" -> new LastAccumulator("id", id),
        "dist" -> new ConstAccumulator[Long](1)
      ))

    val condition =
      new AndCondition(
        new EqualityCondition[Long]("start", id, Constant()),
        new EqualityCondition[Long]("end", 12, Variable()),
        new LessThanOrEqualCondition[Int]("length", 3, Increasing(Some(1)))
    )

    expand(g.getVertex(id), results, condition).toList
  }
}