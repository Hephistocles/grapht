package re.toph.hybrid_db

import java.sql.Connection

import java.util.{Comparator, HashMap, PriorityQueue}
/**
  * Created by christoph on 20/05/16.
  */
class NearestPoint[VertexKey,Vertex,Edge](g : GraphAdaptor[VertexKey,Vertex,Edge])(implicit connection : Connection) {

  case class PartResult(id: VertexKey, node: Vertex, dist: Double, priority:Double, from: PartResult)

  val RADIUS = 6371
  def heuristic(from:Vertex, to:Vertex) : Double =
  {
    val (lat1, lng1) = g.getLatLng(from)
    val (lat2, lng2) = g.getLatLng(to)
    val (lat1Rad, lng1Rad, lat2Rad, lng2Rad) = (
      Math.toRadians(lat1/1000000.0),
      Math.toRadians(lng1/1000000.0),
      Math.toRadians(lat2/1000000.0),
      Math.toRadians(lng1/1000000.0))

    val cLa1 = Math.cos(lat1Rad)
    val A = (
      RADIUS * cLa1 * Math.cos(lng1Rad),
      RADIUS * cLa1 * Math.sin(lng1Rad),
      RADIUS * cLa1 * Math.sin(lat1Rad))

    val cLa2 = Math.cos(lat2Rad)
    val B = (
      RADIUS * cLa2 * Math.cos(lng2Rad),
      RADIUS * cLa2 * Math.sin(lng2Rad),
      RADIUS * cLa2 * Math.sin(lat2Rad))

    val res = Math.sqrt((A._1 - B._1) * (A._1 - B._1) +
      (A._2 - B._2) * (A._2 - B._2) +
      (A._3 - B._3) * (A._3 - B._3))

    res
  }
  def find(start:VertexKey, end:VertexKey): PartResult = {

//    var getCount = 0

    val startVertex = g.getVertex(start)
    val goalNode = g.getVertex(end)

    val openSet = new HashMap[VertexKey, PartResult]()
    val closedSet = new HashMap[VertexKey, PartResult]()
    val openQueue = new PriorityQueue[PartResult](new Comparator[PartResult](){
      override def compare(o1: PartResult, o2: PartResult): Int = o1.priority.compareTo(o2.priority)
    })

    val init = PartResult(start, startVertex, 0d, 0d, null)
    openQueue.add(init)
    openSet.put(start, init)

//    var closedSet = Map[VertexKey, PartResult]()
//    var openSet = Map[VertexKey, PartResult](start -> PartResult(start, getVertex(start), 0d, 0d, null))
//
//    implicit def orderedPartResult(f: PartResult): Ordered[PartResult] = new Ordered[PartResult] {
//      def compare(other: PartResult) = f.f.compare(other.f)
//    }
//
//    val openQueue = mutable.PriorityQueue[PartResult](openSet(start))

//    var popCount = 0
    while (!openQueue.isEmpty()) {
      // pop most likely element
      val q = openQueue.poll()
//      popCount += 1
      val vertex = q.node
      closedSet.put(g.getKey(vertex), q)
//      closedSet += (q.id -> q)

      if (q.id == end) {
        return q
      }

      // get q's successors
      val edges = g.getEdges(vertex)
      edges.foreach(edge => {

        val targetNode : Vertex = g.getTarget(edge)
        val targetKey = g.getKey(targetNode)

        if (closedSet.containsKey(targetKey)) {
          // do nothing
        } else {
          val dist = q.dist + g.getDist(edge)
          val priority = dist + 1000*heuristic(targetNode, goalNode)
          val newPath = PartResult(targetKey, targetNode, dist, priority, q)

          if ( openSet.containsKey(targetKey)){
            if (openSet.get(targetKey).priority > priority) {
              openSet.put(targetKey, newPath)
              openQueue.add(newPath)
            }
          } else {
            openSet.put(targetKey, newPath)
            openQueue.add(newPath)
          }
        }

      })
    }

    null

  }


}
