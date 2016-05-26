package re.toph.hybrid_db
import java.sql.Connection
import java.util.{Comparator, HashMap, PriorityQueue}
import anorm.SQL

import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node, Relationship}
import re.toph.hybrid_db.Neo4JLoader.ROAD

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait GraphAdaptor[K,V,E] {
  def getVertex(k:K) :  V
  def start() : Unit = ()
  def end() : Unit = ()
  def getEdges(k:V) : Iterator[E]
  def getLatLng(v:V) : (Long, Long)
  def getDist(e:E) : Long
  def getTarget(e:E) : V
  def getKey(v:V) : K
}

case class PSQLEdge(id:Long, id1:Long, id2:Long, dist:Long)
class PSQLAdaptor(vertex:String, vertexKey:String, edge:String, id1:String, id2:String)(implicit connection:Connection) extends GraphAdaptor[Long,Long,PSQLEdge] {

  override def getVertex(k: Long): Long = k

  override def getDist(e: PSQLEdge): Long = e.dist

  override def getLatLng(v: Long): (Long, Long) = SQL(s"SELECT lat, lng FROM $vertex WHERE $vertexKey={k}")
                                                    .on("k"->v)().map(r => (r[Long]("lat"), r[Long]("lng"))).head

  override def getEdges(k: Long): Iterator[PSQLEdge] = SQL(s"SELECT * FROM $edge WHERE $id1={k}")
                                                    .on("k"->k)().map(r => PSQLEdge(r[Long]("id"), r[Long](id1), r[Long](id2), r[Long]("dist"))).iterator

  override def getTarget(e: PSQLEdge): Long = e.id2

  override def getKey(k: Long): Long = k
}

class GraphtAdaptor(g:Graph)(implicit connection:Connection) extends GraphAdaptor[Long,GraphNode,Edge] {

  override def getVertex(k: Long): GraphNode = g.getVertex(k)

  override def getDist(e: Edge): Long = e.properties("dist").asInstanceOf[Long]

  override def getLatLng(v: GraphNode): (Long, Long) = (v.properties("lat").asInstanceOf[Long], v.properties("lng").asInstanceOf[Long])

  override def getEdges(k: GraphNode): Iterator[Edge] = k.edges.iterator

  override def getTarget(e: Edge): GraphNode = getVertex(e.to)

  override def getKey(v: GraphNode): Long = v.id
}

class NeoAdaptor()(implicit db: GraphDatabaseService) extends GraphAdaptor[Long, Node, Relationship] {

  val index = db.index().forNodes("junctions")

  override def getVertex(k: Long): Node = index.get("id", k).getSingle()

  override def getDist(e: Relationship): Long = e.getProperty("distance").asInstanceOf[Long]

  override def getLatLng(v: Node): (Long, Long) = (v.getProperty("lat").asInstanceOf[Long], v.getProperty("long").asInstanceOf[Long])

  override def getEdges(k: Node): Iterator[Relationship] = k.getRelationships(ROAD, Direction.OUTGOING).iterator().asScala

  override def getTarget(e: Relationship): Node = e.getEndNode()

  override def getKey(v: Node): Long = v.getProperty("id").asInstanceOf[Long]
}

/**
  * Created by christoph on 20/05/16.
  */
class AStarCalculator[VertexKey,Vertex,Edge](g : GraphAdaptor[VertexKey,Vertex,Edge])(implicit connection : Connection) {

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

  case class Path(dist:Double, length:Int, vertices:List[VertexKey])
  def makeList(p:PartResult): Path = {
    val keys = ListBuffer[VertexKey]()
    var current = p
    var count = -1 // because there is one fewer hop than nodes
    while (current != null) {
      keys += current.id
      current = current.from
      count += 1
    }
    Path(p.dist, count, keys.toList.reverse)
  }

  def find(start:VertexKey, end:VertexKey): Path = {

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
        return makeList(q)
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

