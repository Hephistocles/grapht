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

    val iterations = 5
    var res = List[Map[String, Any]]()

    for (it <- 1 to iterations) {

      Timer.time("Cypher", {
        getHopDistsCypher(4, 20)(connection)
      })

      Timer.time("SQL Joins", {
        getHopDistsSQL(4, 20)(connection)
      })

      //    res = Timer.time("No prefetch", {
      //      getHopDists(5, 20, NullPrefetcher)
      //    })

      res = Timer.time("Lookahead Join (1)", {
        getHopDists(4, 20, new LookaheadJoinPrefetcher(1))
      })

      for (i <- 3 to 5) {
        res = Timer.time("Lookahead Multi (" + i + ")", {
          getHopDists(, 20, new LookaheadMultiPrefetcher(i))
        })
      }
    }

    // clean up
    wsclient.close()

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
        )).toList
  }

//  MATCH path=(p:Point {id:1})-[r:Road*0..3]->(p2)
//  WITH extract(x in r: x.dist) as dists, extract(x in r:x.id2), length(p) as l
//  RETURN p.id as s, p2.id as e, l, reduce(res=0, x in scores: res + x) as dist

  def getHopDistsSQL(hops:Int, n:Long)(implicit connection: Connection) : List[Map[String, Any]] = {
    if (hops != 4) throw new Exception("We only support 4 hops!")
     SQL(
      """
        |SELECT A.id1 s, D.id2 e,
        |   4 l, CONCAT(A.id1, '->', B.id1, '->', C.id1, '->', C.id2, '->', D.id2) p, A.dist + B.dist + C.dist + D.dist dist
        | FROM edges A
        | JOIN edges B
        |   ON A.id2 = B.id1
        | JOIN edges C
        |   ON B.id2 = C.id1
        | JOIN edges D
        |   ON C.id2 = D.id1
        | JOIN points nA ON A.id1=nA.id
        | JOIN points nB ON B.id1=nB.id
        | JOIN points nC ON C.id1=nC.id
        | JOIN points nD ON D.id1=nD.id
        | JOIN points nE ON D.id2=nE.id
        | WHERE A.id1 = {id}
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
      edgeProps = HashMap(
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", "->", "(" + id + ")")
      ),
      vertexProps = HashMap(
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