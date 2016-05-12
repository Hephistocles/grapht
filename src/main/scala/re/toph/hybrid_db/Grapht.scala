package re.toph.hybrid_db

import java.sql.Connection

import scala.collection.mutable.ListBuffer

/**
  * Created by christoph on 12/05/16.
  */
object Grapht {

  case class PartialPath(id: Long, sofar: Result, priority: Int)

  def query(g: Graph, _id: Long, _sofar: Result, condition: CompletionCondition)(implicit connection:Connection): ListBuffer[Map[String, Any]] = {

    val queue = new NodeQueue[PartialPath]()
    queue.add(PartialPath(_id, _sofar, 0))

    val pathResults = ListBuffer[Map[String, Any]]()

    while (!queue.isEmpty()) {
      val part = queue.get()
      val id = part.id
      val sofar = part.sofar

      val vertex = g.getVertex(id)

      // calculate a new intermediate result, taking into account this vertex's properties
      val vertexResult = sofar.mapVertex((acc) => acc.acc(vertex.properties))

      if (condition.check(vertexResult)) {
        pathResults += vertexResult.result
      }

      // Only consider neighbours if it could be beneficial
      if (condition.nextSatisfiable(vertexResult)) {

        // Otherwise let's look at neighbours
        vertex.edges.foreach(e => {
          val edgeResult = vertexResult.mapEdges(a => a.acc(e.properties))

          // explore neighbours to find more path results and merge the results in
          // TODO: replcae priority 0 with something meaningful
          queue.add(PartialPath(e.to, edgeResult, 0))
        })
      }
    }

    pathResults
  }


}
