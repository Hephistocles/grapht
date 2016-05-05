package re.toph.hybrid_db

import scala.collection.immutable.HashMap


/**
  * Created by christoph on 30/04/16.
  */
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
