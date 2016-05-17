package re.toph.hybrid_db

import java.sql.Connection
import java.util
import java.util.{Comparator, PriorityQueue}

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 12/05/16.
  */
object Grapht {

  case class PartialPath(id: Long, sofar: Result, priority: Double)

  def query(g: Graph, _id: Long, _sofar: Result, condition: CompletionCondition, prioritiser: (Edge, GraphNode, Result) => Double)(implicit connection:Connection): ListBuffer[Map[String, Any]] = {

    val limit = 1
    val open = new util.HashMap[Long, PartialPath]()
    val closed = new util.HashMap[Long, PartialPath]()

//    val queue = new NodeQueue[PartialPath]()
    val queue = new PriorityQueue[PartialPath](new Comparator[PartialPath](){
      override def compare(o1: PartialPath, o2: PartialPath): Int = o1.priority.compareTo(o2.priority)
    })

    val init = PartialPath(_id, _sofar, 0)
    queue.add(init)
    open.put(_id, init)


    val pathResults = ListBuffer[Map[String, Any]]()

    while (!queue.isEmpty()) {
//      val part = queue.get()
      val part = queue.poll()
      val id = part.id
      val sofar = part.sofar
//      println(s"    Looking at $id")

      val vertex = g.getVertex(id)
      closed.put(id, part)

      // calculate a new intermediate result, taking into account this vertex's properties
      val vertexResult = sofar.mapVertex((acc) => acc.acc(vertex.properties))

      if (condition.check(vertexResult)) {
        pathResults += vertexResult.result
      }

      // check we haven't overstepped our limits
      if (pathResults.length >= limit)
        return pathResults

      // Only consider neighbours if it could be beneficial
      if (condition.nextSatisfiable(vertexResult)) {

        // Otherwise let's look at neighbours
        vertex.edges.foreach(e => {
          val edgeResult = vertexResult.mapEdges(a => a.acc(e.properties))

          // NB: this could hurt if we don't actually need the vertex!
          val priority = prioritiser(e, g.getVertex(e.to), edgeResult)
          val newPath = PartialPath(e.to, edgeResult, priority)

          if (open.containsKey(e.to)) {
            if (open.get(e.to).priority > priority) {
              queue.remove(open.get(e.to))
              open.remove(e.to)

              open.put(e.to, newPath)
              queue.add(newPath)
            }
          // have we visited this node before?
          //   IF we have, only re-examine if the cost this time is lower than last time
          } else if (closed.containsKey(e.to)) {
            if (closed.get(e.to).priority > priority) {
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
