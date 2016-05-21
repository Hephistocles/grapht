package re.toph.hybrid_db

import java.util.{PriorityQueue, Comparator, HashMap}

import java.sql.Connection

/**
  * Created by christoph on 20/05/16.
  */
class ASFAWEF(g : Graph)(implicit connection : Connection) {

  type VertexKey  = Long
  type Vertex     = GraphNode
  type EdgeKey    = Long
  case class PartResult(id: VertexKey, node: Vertex, dist: Double, priority:Double, from: PartResult)

  def getVertex(k : VertexKey): Vertex = {
    g.getVertex(k)
  }

  def getEdges(k : Vertex): List[Edge] = {
    k.edges
  }

  val RADIUS = 6371
  def heuristic(from:Vertex, to:Vertex) : Double =
  {
    val (lat1, lng1, lat2, lng2) = (
      Math.toRadians(from.properties("lat").asInstanceOf[Long]/1000000.0),
      Math.toRadians(from.properties("lng").asInstanceOf[Long]/1000000.0),
      Math.toRadians(to.properties("lat").asInstanceOf[Long]/1000000.0),
      Math.toRadians(to.properties("lng").asInstanceOf[Long]/1000000.0))

    val cLa1 = Math.cos(lat1)
    val A = (
      RADIUS * cLa1 * Math.cos(lng1),
      RADIUS * cLa1 * Math.sin(lng1),
      RADIUS * cLa1 * Math.sin(lat1))

    val cLa2 = Math.cos(lat2)
    val B = (
      RADIUS * cLa2 * Math.cos(lng2),
      RADIUS * cLa2 * Math.sin(lng2),
      RADIUS * cLa2 * Math.sin(lat2))

    val res = Math.sqrt((A._1 - B._1) * (A._1 - B._1) +
      (A._2 - B._2) * (A._2 - B._2) +
      (A._3 - B._3) * (A._3 - B._3))


    res
  }
//    // A*
//    initialize the open list
//      initialize the closed list
//      put the starting node on the open list (you can leave its f at zero)
//
//  while the open list is not empty
//    find the node with the least f on the open list, call it "q"
//  pop q off the open list
//    generate q's 8 successors and set their parents to q
//  for each successor
//  if successor is the goal, stop the search
//  successor.g = q.g + distance between successor and q
//  successor.h = distance from goal to successor
//  successor.f = successor.g + successor.h
//
//  if a node with the same position as successor is in the OPEN list \
//  which has a lower f than successor, skip this successor
//  if a node with the same position as successor is in the CLOSED list \
//  which has a lower f than successor, skip this successor
//  otherwise, add the node to the open list
//  end
//  push q on the closed list
//    end

  def find(start:VertexKey, end:VertexKey): PartResult = {

    val goalNode = getVertex(end)
    val openSet = new HashMap[Long, PartResult]()
    val closedSet = new HashMap[Long, PartResult]()
    val openQueue = new PriorityQueue[PartResult](new Comparator[PartResult](){
      override def compare(o1: PartResult, o2: PartResult): Int = o1.priority.compareTo(o2.priority)
    })

    val startVertex = getVertex(start)
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

    var popCount = 0
    while (!openQueue.isEmpty()) {
      // pop most likely element
      val q = openQueue.poll()
      popCount +=1
      val vertex = q.node
      closedSet.put(vertex.id, q)
//      closedSet += (q.id -> q)

      if (q.id == end) {
        return q}

      // get q's successors
      vertex.edges.foreach(edge => {

        if (closedSet.containsKey(edge.to)) {
          // do nothing
        } else {
          val targetNode = getVertex(edge.to)
          val dist = q.dist + edge.properties("dist").asInstanceOf[Long]
          val priority = dist + 1000*heuristic(targetNode, goalNode)
          val newPath = PartResult(edge.to, targetNode, dist, priority, q)

          if ( openSet.containsKey(edge.to)){
            if (openSet.get(edge.to).priority > priority) {
              openSet.put(edge.to, newPath)
              openQueue.add(newPath)
            }
          } else {
            openSet.put(edge.to, newPath)
            openQueue.add(newPath)
          }
        }

      })
    }

    null

  }


}
