package re.toph.hybrid_db

import java.sql.Connection
import java.util
import java.util.{Comparator, PriorityQueue}

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 12/05/16.
  */
object Grapht {

  case class PartialPath(lastVertex : GraphNode, sofar: Result, priority: Double)

  def query(g: Graph, _id: Long, _sofar: Result, condition: CompletionCondition, prioritiser: (Edge, GraphNode, Result) => Double, limit :Int = 1)(implicit connection:Connection): ListBuffer[Map[String, Any]] = {

    // TODO: I don't think this handles uniqueness at all

    // create data structures
    val open = new util.HashMap[Long, PartialPath]()
    val closedVertices = new util.HashMap[Long, PartialPath]()
    val queue = new PriorityQueue[PartialPath](new Comparator[PartialPath](){
      override def compare(o1: PartialPath, o2: PartialPath): Int = o1.priority.compareTo(o2.priority)
    })
    val pathResults = ListBuffer[Map[String, Any]]()

    // initialise start vertex
    val startVertex = g.getVertex(_id)
    val initResult = _sofar.mapVertex((acc) => acc.acc(startVertex.properties))
    val init = PartialPath(startVertex, initResult, 0)
    queue.add(init)
    open.put(_id, init)

    while (!queue.isEmpty()) {

      val part = queue.poll()
      val vertex = part.lastVertex
      val sofar = part.sofar
      closedVertices.put(vertex.id, part)

      if (condition.check(sofar)) {
        pathResults += sofar.result

        // check we haven't overstepped our limits
        if (pathResults.length >= limit)
          return pathResults
      }

      // Only consider neighbours if it could be beneficial
      if (condition.nextSatisfiable(sofar)) {

        // Otherwise let's look at neighbours
        vertex.edges.foreach(e => {
          val edgeResult = sofar.mapEdges(a => a.acc(e.properties))

          // NB: this could hurt if we don't actually need the vertex!
          val toVertex = g.getVertex(e.to)
          val priority = prioritiser(e, toVertex, edgeResult)
          val endPointResult = edgeResult.mapVertex(a => a.acc(toVertex.properties))
          val newPath = PartialPath(toVertex, endPointResult, priority)

          if (open.containsKey(e.to)) {
            if (open.get(e.to).priority > priority) {
              queue.remove(open.get(e.to))
              open.remove(e.to)

              open.put(e.to, newPath)
              queue.add(newPath)
            }
          // have we visited this node before?
          //   IF we have, only re-examine if the cost this time is lower than last time
          } else if (closedVertices.containsKey(e.to)) {
            if (closedVertices.get(e.to).priority > priority) {
              open.put(e.to, newPath)
              queue.add(newPath)
            }
          // Or if this is the first time we've seen this node, schedule it as normal
          } else {
            open.put(e.to, newPath)
            queue.add(newPath)
          }
        })
      }
    }

    pathResults
  }


}
