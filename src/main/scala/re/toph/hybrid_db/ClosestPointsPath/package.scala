package re.toph.hybrid_db

import java.sql.Connection

import org.anormcypher.Neo4jREST
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import play.api.libs.ws.ning.NingWSClient

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext




package object ClosestPointsPath extends BenchmarkTest {

  val RADIUS = 6371
  def latlongdist(latitude1:Double, longitude1:Double, latitude2:Double, longitude2:Double ) : Double =
  {
    val (lat1, lng1, lat2, lng2) = (
      Math.toRadians(latitude1/1000000.0),
      Math.toRadians(longitude1/1000000.0),
      Math.toRadians(latitude2/1000000.0),
      Math.toRadians(longitude2/1000000.0))

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

  val (hops, id) = (4, 1)
  val (lat1, lng1) = (-73655377.0490897, 40941211.5560335)

  def neoFindClosest(lat1: Double, lng1: Double)(implicit db: GraphDatabaseService) = {

    val tx = db.beginTx()
    try {
      var min = Double.MaxValue
      var minNode: Node = null
      db.getAllNodes().asScala.foreach(n => {
        val dist = latlongdist(n.getProperty("lat").asInstanceOf[Long].toDouble, n.getProperty("lat").asInstanceOf[Long].toDouble, lat1, lng1)
        //            println(lat)
        if (dist < min) {
          min = dist
          minNode = n
        }
      })
      tx.success()
      minNode
    } finally {
      tx.close()
    }
  }

  def go()(implicit db: GraphDatabaseService, connection:Connection, connection2: Neo4jREST, wsclient:NingWSClient, ec:ExecutionContext): Unit = {

    //TODO: lookahead join doesn't work atm. Please try again later.
    val (lookaheads, blocks) = (
      (4 to 4)
        .map(b => (s"Lookahead ($b)", new LookaheadMultiPrefetcher(b))),
      List(1, 10, 100, 1000, 5000, 10000, 20000)
        .map(b => (s"Block ($b)", new LookaheadBlockPrefetcher(b))))

    val prefetchers = List() ++ lookaheads //++ blocks

    val tests = List(
      ("Neo Find Closest", () => neoFindClosest(lat1, lng1))
    )
//    ++ prefetchers.toList.map({
//      case (s, p) => (s"GRAPHT $s", () => getHopDists(hops, id, p))
//    })

    timeAll(tests.toList, 5)
  }
}
