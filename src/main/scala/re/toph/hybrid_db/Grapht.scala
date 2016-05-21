package re.toph.hybrid_db

import java.sql.Connection
import java.util
import java.util.{Comparator, PriorityQueue}

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 12/05/16.
  */
object Grapht {

  case class PartialPath(lastVertex : GraphNode, sofar: Result, priority: Double, closed: Set[Long])

  def query(g: Graph, _id: Long, _sofar: Result, condition: CompletionCondition, prioritiser: (Edge, GraphNode, Result) => Double, limit :Int = 2)(implicit connection:Connection): ListBuffer[Map[String, Any]] = {

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
    val init = PartialPath(startVertex, initResult, 0, Set(startVertex.id))
    queue.add(init)
    open.put(_id, init)
    var popCount = 0

    while (!queue.isEmpty()) {

      val part = queue.poll()
      val vertex = part.lastVertex
      val sofar = part.sofar
      closedVertices.put(vertex.id, part)
      popCount += 1

      if (condition.check(sofar)) {
        pathResults += sofar.result

        // check we haven't overstepped our limits
        if (pathResults.length >= limit) {
          println(popCount)
          return pathResults}
      }

      // Only consider neighbours if it could be beneficial
      if (condition.nextSatisfiable(sofar)) {

        // Otherwise let's look at neighbours
        vertex.edges.foreach(e => {

          // TODO: only do this if we requested unique vertices
          if (part.closed.contains(e.to))  {
            // I think do nothing
          } else {

            // NB: this could hurt if we don't actually need the vertex!
            val toVertex = g.getVertex(e.to)

            val edgeResult = sofar.mapEdges(a => a.acc(e.properties))
            val priority = prioritiser(e, toVertex, edgeResult)
            val endPointResult = edgeResult.mapVertex(a => a.acc(toVertex.properties))
            val newPath = PartialPath(toVertex, endPointResult, priority, part.closed + e.to)

            if (open.containsKey(e.to)) {
              if (limit!=1
                // if the limit is 1, then we should discard repeated paths (and become A*)
                // if not, we need every path we can get!
                || open.get(e.to).priority > priority) {

                open.put(e.to, newPath)
                queue.add(newPath)
              }
            } else {
              open.put(e.to, newPath)
              queue.add(newPath)
            }
          }


        })
      } else {
        println("Avoided a search")
      }
    }

    pathResults
  }


}
