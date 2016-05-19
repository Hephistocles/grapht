package re.toph.hybrid_db

import java.sql.Connection

import anorm.{SqlParser, SQL}
import org.anormcypher.{Cypher, Neo4jREST}
import org.neo4j.graphalgo.{CommonEvaluators, EstimateEvaluator, GraphAlgoFactory}
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node, PathExpanders}
import play.api.libs.ws.ning.NingWSClient
import scala.collection.JavaConverters._
import re.toph.hybrid_db.Neo4JLoader.ROAD

import scala.concurrent.ExecutionContext

/**
  * Created by christoph on 12/05/16.
  */

package object AStar extends BenchmarkTest {


  val RADIUS = 6371
  def heuristic(latitude1:Long, longitude1:Long, latitude2:Long, longitude2:Long ) : Double =
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

  val (hops, id) = (3, 1)

  def go()(implicit db: GraphDatabaseService, connection:Connection,connection2: Neo4jREST, wsclient:NingWSClient, ec:ExecutionContext): Unit = {

//    //TODO: lookahead join doesn't work atm. Please try again later.
    val lookaheads = (45 to 50 by 5)
        .map(b => (s"Lookahead ($b)", new LookaheadMultiPrefetcher(b)))

//    val blocks = List(20000, 30000,50000)
//        .map(b => (s"Block ($b)", new LookaheadBlockPrefetcher(b)))

    val prefetchers = lookaheads // ++

    val (from, to) = (1, 50)
    val tests = /* List(
      ("East to West Neo", () => neo2()),
      ("East to West No Search", () => neo(58197, 4362)),
        ("Neo", () => neo(from, to)),
        ("PSQL", () => psql(from, to))
//      ("SQL", () => unionSQL(hops, id)),
//      ("SQL CTE", () => CTESQL(hops, id))
    ) ++ */ prefetchers.flatMap({
      case (s, p) =>
        List(
//          (s"Grapht $s", () => grapht(from, to, p)),
          (s"Grapht Local $s", () => graphtLocal(from, to, p))
        )
    })

    timeAll(tests.toList, 3)
  }

  def neo2()(implicit db: GraphDatabaseService): (Int,Double) = {
    val tx = db.beginTx()
    try {

      var (i, j) = (Long.MaxValue, Long.MinValue)
      var (minNode, maxNode):(Node, Node) = (null, null)
      db.getAllNodes().asScala.foreach(n => {
        val lat = n.getProperty("lat").asInstanceOf[Long]
        //            println(lat)
        if (lat<i) {
          i = lat
          minNode = n
        }
        if (j<lat) {
          j = lat
          maxNode = n
        }
      })
      println(minNode, maxNode)

      val path = GraphAlgoFactory.aStar(
        PathExpanders.forTypeAndDirection(ROAD, Direction.OUTGOING),
        CommonEvaluators.doubleCostEvaluator("distance"),
        new EstimateEvaluator[java.lang.Double] {
          override def getCost(node1: Node, node2: Node): java.lang.Double = heuristic(
            node1.getProperty("lat").asInstanceOf[Long],
            node1.getProperty("long").asInstanceOf[Long],
            node2.getProperty("lat").asInstanceOf[Long],
            node2.getProperty("long").asInstanceOf[Long]
          )
        }
      ).findSinglePath(minNode, maxNode)

      tx.success()
      println(s"${path.weight()} in ${path.length()} hops")
      println(path)
      (path.length(), path.weight())
    } finally {
      tx.close()
    }
  }

  def neo(from:Long, to:Long)(implicit db: GraphDatabaseService): (Long, List[Long]) = {
    val tx = db.beginTx()
    try {

      val junctionIndex =  db.index().forNodes("junctions")
      val start = junctionIndex.get("id", from).getSingle()
      val end = junctionIndex.get("id", to).getSingle()

      val path = GraphAlgoFactory.aStar(
        PathExpanders.forTypeAndDirection(ROAD, Direction.OUTGOING),
        CommonEvaluators.doubleCostEvaluator("distance"),
        new EstimateEvaluator[java.lang.Double] {
          override def getCost(node1: Node, node2: Node): java.lang.Double = heuristic(
            node1.getProperty("lat").asInstanceOf[Long],
            node1.getProperty("long").asInstanceOf[Long],
            node2.getProperty("lat").asInstanceOf[Long],
            node2.getProperty("long").asInstanceOf[Long]
          )
        }
      ).findSinglePath(start, end)

      tx.success()
      val rs = (path.weight().toLong, path.nodes().asScala.toList.map(_.getProperty("id").asInstanceOf[Long]))
      println(s"Neo : ${rs._1} in ${rs._2.length} hops: ${rs._2.mkString("->")}")
      rs
    } finally {
      tx.close()
    }
  }

  def cypher(hops: Int, n: Long)(implicit connection: Neo4jREST, wsclient:NingWSClient, ec:ExecutionContext): List[Map[String, Option[AnyVal]]] = {
    Cypher(
      """MATCH path=(p:Point {id:{id}})-[r:Road*0..""" + hops +
        """]->(p2)
          |WITH p, p2, extract(x in r| x.dist) as dists,
          |length(path) as l
          |RETURN p.id as s, p2.id as e, l,
          |reduce(res=0, x in dists | res + x) as dist""".stripMargin)
      .on("id" -> n, "hops" -> hops)
      .apply()
      //    .foreach(println)
      .map(row =>
      Map(
        "s" -> row[Option[Long]]("s"),
        "e" -> row[Option[Long]]("e"),
        "l" -> row[Option[Int]]("l"),
        //          "p"->row[String]("p"),
        "dist" -> row[Option[Long]]("dist")
      ))
      .filter(m => m("s") match {
        case Some(x) => true
        case None => false
      })
      .toList
  }

  def psql(from:Long, to: Long)(implicit connection: Connection): (Long, List[Long]) = {
    val rs = SQL(
      "SELECT distance::bigint, path::bigint[] FROM (SELECT (astarroute({from}, {to})).*) t;")
      .on("from" -> from, "to" -> to)()
      .map((row) => {
        (row[Long]("distance"), row[Array[Long]]("path").toList)
      }).head

    println(s"PSQL: ${rs._1} in ${rs._2.length} hops: ${rs._2.mkString("->")}")
    rs
  }

  def sqlheuristic(lat1:Long, lng1:Long, lat2:Long, lng2:Long)(implicit conenction:Connection) = {
    SQL("SELECT latlongdist({a},{b},{c},{d})")
    .on("a"-> lat1, "b"->lng1, "c"->lat2, "d"->lng2)()
    .toList
  }

  def CTESQL(hops: Int, n: Long)(implicit connection: Connection): List[Map[String, Any]] = {
    SQL(
      """
        |-- calculate generation / depth, no updates
        |WITH RECURSIVE ans
        |  (s, e, l, p, dist)
        | AS ( SELECT {id}, {id}, 0, CONCAT('(', {id},')'), 0::bigint
        |
        |      UNION ALL
        |
        |      SELECT ans.s, edges.id2, ans.l+1, CONCAT(ans.p, '->', edges.id2), ans.dist + edges.dist
        |      FROM edges
        |      JOIN ans ON ans.e=edges.id1
        |      WHERE ans.l < {hops}
        |    )
        |SELECT * FROM ans;
      """.stripMargin)
      .on("id" -> n, "hops" -> hops)
      .apply()
      .map(row =>
        Map(
          "s" -> row[Long]("s"),
          "e" -> row[Long]("e"),
          "l" -> row[Int]("l"),
          "p" -> row[String]("p"),
          "dist" -> row[Long]("dist")
        )).toList
  }

  /*
    SELECT start, end, length, (ACC VERTICES CONCAT id " -> ") path, (ACC EDGES SUM dist) cost FROM
    PATHS OVER myGraph
    WHERE START = {from}
    AND END = {to}
    TRAVERSE UNIQUE VERTICES BY MIN cost + latlongdist(VERTEX(lat), VERTEX(lng), myLat, myLng)
    LIMIT 1;
  */

  def graphtLocal(from:Long, to:Long, p: Prefetcher)(implicit connection:Connection): (Long, Array[Long]) = {
    // first get the lat/lng of the target node
    val (targetLat, targetLong) = SQL("SELECT lat, lng FROM points WHERE id={id}")
      .on("id"->to)
      .apply()
      .map(r => (r[Long]("lat"), r[Long]("lng"))).head

    val g = new Graph(p)

    val results = new Result(
      edgeProps = Map[String, TAccumulator](
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", ",", id.toString()),
        "dist" -> new SumAccumulator[Long]("dist", 0)
      ),
      vertexProps = Map[String, TAccumulator](
        "start" -> new ConstAccumulator(from),
        "end" -> new LastAccumulator("id", from)
      ))

    def prioritiser(edge:Edge, vertex:GraphNode, sofar: Result)(implicit connection:Connection): Double = {
      val h = heuristic(vertex.properties("lat").asInstanceOf[Long],vertex.properties("lng").asInstanceOf[Long], targetLat, targetLong)
      //      + edge.properties("dist").asInstanceOf[Long]
      val g = sofar.get("dist").asInstanceOf[Long]

      h + g
    }


    val condition =
      new AndCondition(
        new EqualityCondition[Long]("start", from, Constant()),
        new EqualityCondition[Long]("end", to, Variable())
        //        new LessThanOrEqualCondition[Int]("length", 3, Increasing(Some(1)))
      )

    val rs = Grapht.query(g, id, results, condition, prioritiser)
      .map({
        m => (m("dist").asInstanceOf[Long], m("path").asInstanceOf[String].split(",").map(_.toLong))
      }).head

    println(s"GftL: ${rs._1} in ${rs._2.length} hops: ${rs._2.mkString("->")}")
    rs
  }

  def grapht(from:Long, to:Long, p: Prefetcher)(implicit connection:Connection): (Long, Array[Long]) = {
    // first get the lat/lng of the target node
    val (targetLat, targetLong) = SQL("SELECT lat, lng FROM points WHERE id={id}")
      .on("id"->to)
      .apply()
      .map(r => (r[Long]("lat"), r[Long]("lng"))).head

    val g = new Graph(p)

    val results = new Result(
      edgeProps = Map[String, TAccumulator](
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", ",", id.toString()),
        "dist" -> new SumAccumulator[Long]("dist", 0)
      ),
      vertexProps = Map[String, TAccumulator](
        "start" -> new ConstAccumulator(from),
        "end" -> new LastAccumulator("id", from)
      ))

    def prioritiser(edge:Edge, vertex:GraphNode, sofar: Result)(implicit connection:Connection): Double = {
      val h = Timer.time("DB ll", {
        SQL("SELECT latlongdist({lat}, {lng}, {gLat}, {gLng}) AS priority")
          //      .on("lat" -> 1L, "lng"->1L)
          .on("lat" -> vertex.properties("lat").asInstanceOf[Long], "lng"->vertex.properties("lng").asInstanceOf[Long], "gLat"->targetLat, "gLng"->targetLong)
          .as(SqlParser.double("priority").single)
      })
      //      + edge.properties("dist").asInstanceOf[Long]
      val g = sofar.get("dist").asInstanceOf[Long]

      h + g
    }


    val condition =
      new AndCondition(
        new EqualityCondition[Long]("start", from, Constant()),
        new EqualityCondition[Long]("end", to, Variable())
        //        new LessThanOrEqualCondition[Int]("length", 3, Increasing(Some(1)))
      )

    val rs = Grapht.query(g, id, results, condition, prioritiser, 1)
      .map({
        m => (m("dist").asInstanceOf[Long], m("path").asInstanceOf[String].split(",").map(_.toLong))
      }).head

    println(s"Gpht: ${rs._1} in ${rs._2.length} hops: ${rs._2.mkString("->")}")
    rs
  }
}

