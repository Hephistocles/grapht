package re.toph.hybrid_db

import java.sql.Connection

import anorm.SQL
import org.anormcypher.{Cypher, Neo4jREST}
import org.neo4j.graphdb.traversal.{Evaluators, Uniqueness}
import org.neo4j.graphdb.{Direction, GraphDatabaseService}
import play.api.libs.ws.ning.NingWSClient

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

/**
  * Created by christoph on 12/05/16.
  */
trait BenchmarkTest {
  val timeAll: (List[(String, ()=>List[Map[String,Any]])], Int) => Unit =
    (l, iterations) => {
      for (i <- 0 to iterations) {
        l.foreach({
          case (s, f) => Timer.time(s, f(), printResults=false)
        })
      }
    }
}


package object FriendOfFriend extends BenchmarkTest {

  val (hops, id) = (3, 1)

  def go()(implicit db: GraphDatabaseService, connection:Connection,connection2: Neo4jREST, wsclient:NingWSClient, ec:ExecutionContext): Unit = {
    val prefetchers:ListBuffer[(String, Prefetcher)] = ListBuffer(
    )

    //TODO: lookahead join doesn't work atm
//    prefetchers ++= (1 to 5)
//      .map(b => (s"Lookahed Join ($b)", new LookaheadJoinPrefetcher(b)))
    prefetchers ++= (3 to 5)
      .map(b => (s"Lookahead ($b)", new LookaheadMultiPrefetcher(b)))
    prefetchers ++= List(1, 10, 100, 1000, 5000, 10000, 20000)
      .map(b => (s"Block ($b)", new LookaheadBlockPrefetcher(b)))

    val tests = ListBuffer(
      ("Neo", () => getHopDistsNeo(hops, id)),
      ("Cypher", () => getHopDistsCypher(hops, id)),
      ("SQL", () => getHopDistsSQL(hops, id)),
      ("SQL CTE", () => getHopDistsCTESQL(hops, id))
    )

    tests ++= prefetchers.toList.map({
      case (s, p) => (s"** GRAPHT $s", () => getHopDists(hops, id, p))
    })

    timeAll(tests.toList, 3)
  }

  def getHopDistsNeo(hops:Int, n:Long)(implicit db: GraphDatabaseService):List[Map[String, Any]] = {
    val tx = db.beginTx()
    try {

      val junctionIndex =  db.index().forNodes("junctions")
      val start = junctionIndex.get("id", n).getSingle()

      val td = db.traversalDescription()
        .depthFirst()
        .relationships(Neo4JLoader.ROAD, Direction.OUTGOING)
        .uniqueness(Uniqueness.NONE)
        .evaluator(Evaluators.toDepth(hops))

      val res =
        Timer.time("Query", {
          td.traverse(start).iterator()
        })
          .asScala.map(r => {
          Map(
            "s" -> r.startNode().getProperty("id"),
            "e" -> r.endNode().getProperty("id"),
            "l" -> r.length(),
            "p" -> r.nodes().asScala.tail.foldLeft(n.toString())((a, r) => s"$a -> " + r.getProperty("id")),
            "dist" -> r.relationships().asScala.foldLeft(0L)((a, r) => a + r.getProperty("distance").asInstanceOf[Long])
          )
        }).toList

      tx.success()
      res
    } finally {
      tx.close()
    }
  }

  def getHopDistsCypher(hops: Int, n: Long)(implicit connection: Neo4jREST, wsclient:NingWSClient, ec:ExecutionContext): List[Map[String, Option[AnyVal]]] = {
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

  def getHopDistsSQL(hops: Int, n: Long)(implicit connection: Connection): List[Map[String, Any]] = {
    SQL(
      """
        |(SELECT A.id1 s, D.id2 e, 4 l, 'Something' p, A.dist + B.dist + C.dist + D.dist dist FROM
        | edges A
        |   JOIN edges B ON A.id2=B.id1
        |   JOIN edges C ON B.id2=C.id1
        |   JOIN edges D ON C.id2=D.id1
        |   WHERE A.id1={id})
        |UNION
        |(SELECT A.id1 s, C.id2 e, 3 l, 'Something' p, A.dist + B.dist + C.dist dist FROM
        | edges A
        |   JOIN edges B ON A.id2=B.id1
        |   JOIN edges C ON B.id2=C.id1
        |   WHERE A.id1={id})
        |UNION
        |(SELECT A.id1 s, B.id2 e, 2 l, 'Something' p, A.dist + B.dist dist FROM
        | edges A
        |   JOIN edges B ON A.id2=B.id1
        |   WHERE A.id1={id})
        |UNION
        |(SELECT A.id1 s, A.id2 e, 1 l, 'Something' p, A.dist dist FROM
        | edges A
        |   WHERE A.id1={id})
        |UNION
        |(SELECT {id} s, {id} e, 0 l, 'Something' p, 0 dist)
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

  def getHopDistsCTESQL(hops: Int, n: Long)(implicit connection: Connection): List[Map[String, Any]] = {
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
    WHERE start = ?
    AND length <= ?);
  */
  def getHopDists(n: Int, id: Long, p: Prefetcher)(implicit connection:Connection): List[Map[String, Any]] = {
    val g = new Graph(p)

    val results = new Result(
      edgeProps = Map[String, TAccumulator](
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", "->", "(" + id + ")")
      ),
      vertexProps = Map[String, TAccumulator](
        "start" -> new ConstAccumulator(id),
        "end" -> new LastAccumulator("id", id),
        "dist" -> new SumAccumulator("dist", 0L)
      ))

    val condition =
      new AndCondition(
        new EqualityCondition[Long]("start", id, Constant()),
        new LessThanOrEqualCondition[Int]("length", n, Increasing(Some(1)))
      )

    Grapht.query(g, id, results, condition).toList
  }

}
