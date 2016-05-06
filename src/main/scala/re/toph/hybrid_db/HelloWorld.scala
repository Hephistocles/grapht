package re.toph.hybrid_db

import java.sql.{DriverManager, Connection}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import anorm.{ SQL, SqlParser }
import org.anormcypher._
import play.api.libs.ws._

/**
  * Created by christoph on 28/04/16.
  */
object HelloWorld {

  val driver = "org.postgresql.Driver"
  val url    = "jdbc:postgresql://localhost/christoph"
  val user   = "christoph"
  val pass   = "LockDown1"

  Class.forName(driver)
  implicit val connection:Connection =  DriverManager.getConnection(url, user, pass)
  implicit val wsclient = ning.NingWSClient()
  implicit val connection2 = Neo4jREST("localhost", 7474, "neo4j", pass)

  // Provide an ExecutionContext
  implicit val ec = scala.concurrent.ExecutionContext.global

  def main(args: Array[String]): Unit = {

    val iterations = 1
    val hops=1
    val id=1
    var res = List[Map[String, Any]]()

    try {
      for (it <- 1 to iterations) {

        Timer.time("Cypher", {
          getHopDistsCypher(hops, id)
        })
//          .foreach(println)

        Timer.time("SQL Joins", {
          getHopDistsSQL(hops, id)
        })
//          .foreach(println)

        Timer.time("Lookahead Join", {
          getHopDists(hops, id, new LookaheadJoinPrefetcher(3))
        })
          .foreach(println)

        for (i <- 3 to 6) {
          Timer.time("Lookahead Multi (" + i + ")", {
            getHopDists(hops, id, new LookaheadMultiPrefetcher(i))
          })
//            .foreach(println)
        }
      }
    } catch {
      case e : Throwable => e.printStackTrace()
    } finally {
      wsclient.close()
    }

  }

  def getHopDistsCypher(hops:Int, n:Long)(implicit connection: Connection) = {
    Cypher(
      """MATCH path=(p:Point {id:{id}})-[r:Road*0..""" + hops + """]->(p2)
        |WITH p, p2, extract(x in r| x.dist) as dists,
        |length(path) as l
        |RETURN p.id as s, p2.id as e, l,
        |reduce(res=0, x in dists | res + x) as dist""".stripMargin)
    .on("id" -> n, "hops" -> hops)
    .apply()
//    .foreach(println)
      .map(row =>
        Map(
          "s"->row[Option[Long]]("s"),
          "e"->row[Option[Long]]("e"),
          "l"->row[Option[Int]]("l"),
//          "p"->row[String]("p"),
          "dist"->row[Option[Long]]("dist")
        ))
      .filter(m=>m("s") match {
        case Some(x) => true
        case None => false
      })
      .toList
  }

//  MATCH path=(p:Point {id:1})-[r:Road*0..3]->(p2)
//  WITH extract(x in r: x.dist) as dists, extract(x in r:x.id2), length(p) as l
//  RETURN p.id as s, p2.id as e, l, reduce(res=0, x in scores: res + x) as dist

  def getHopDistsSQL(hops:Int, n:Long)(implicit connection: Connection) : List[Map[String, Any]] = {
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
    .on("id"->n, "hops" -> hops)
    .apply()
       .map(row =>
         Map(
           "s"->row[Long]("s"),
           "e"->row[Long]("e"),
           "l"->row[Int]("l"),
           "p"->row[String]("p"),
           "dist"->row[Long]("dist")
         )).toList
  }

  /*
    SELECT start, end, length, (ACC VERTICES CONCAT id " -> ") path, (ACC EDGES SUM dist) cost FROM
    PATHS OVER myGraph
    WHERE start = ?
    AND length <= ?);
  */
  def getHopDists(n: Int, id: Long, p: Prefetcher): List[Map[String, Any]] = {
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

    query(g, id, results, condition).toList
  }


  def query(g: Graph, id: Long, sofar: Result, condition: CompletionCondition): ListBuffer[Map[String, Any]] = {

    val vertex = g.getVertex(id)

    // calculate a new intermediate result, taking into account this vertex's properties
    val vertexResult = sofar.mapVertex((acc) => acc.acc(vertex.properties))

    val pathResults = ListBuffer[Map[String, Any]]()

    if (condition.check(vertexResult)) {
      pathResults += vertexResult.result
    }

    // if there's no point in constructing longer paths - stop!
    if (!condition.nextSatisfiable(vertexResult)) {
      return pathResults
    }

    // Otherwise let's look at neighbours
    vertex.edges.foreach(e => {
      val edgeResult = vertexResult.mapEdges(a => a.acc(e.properties))

      // explore neighbours to find more path results and merge the results in!
      // TODO: generalise this with a priority queue or something
      //       also important to avoid stackoverflows on large expansions
      pathResults ++= query(g, e.to, edgeResult, condition)
    })

    pathResults
  }

}