package re.toph.hybrid_db

import java.sql.{DriverManager, Connection}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer


/**
  * Created by christoph on 28/04/16.
  */
object HelloWorld {

  def main(args: Array[String]): Unit = {

    val iterations = 3
    var res = List[Map[String, Any]]()

    for (it <- 0 to iterations) {

      Timer.time("SQL Joins", {
        getHopDistsSQL()
      })

      //    res = Timer.time("No prefetch", {
      //      getHopDists(5, 20, NullPrefetcher)
      //    })

      res = Timer.time("Lookahead Join (1)", {
        getHopDists(5, 20, new LookaheadJoinPrefetcher(1))
      })

      for (i <- 3 to 5) {
        res = Timer.time("Lookahead Multi (" + i + ")", {
          getHopDists(4, 1, new LookaheadMultiPrefetcher(i))
        })
      }
    }
//    res.foreach(println)
  }

  val driver = "org.postgresql.Driver"
  val url    = "jdbc:postgresql://localhost/christoph"
  val user   = "christoph"
  val pass   = "LockDown1"

  Class.forName(driver)
  val connection:Connection =  DriverManager.getConnection(url, user, pass)
  def getHopDistsSQL2() = {
    val s = connection.createStatement()
    val rs = s.executeQuery(
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
        | WHERE A.id1 = 1
      """.stripMargin)
    //    while (rs.next()) {
    //      println(rs.getString("p"))
    //    }
  }

  def getHopDistsSQL() = {
    val s = connection.createStatement()
    val rs = s.executeQuery(
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
        | WHERE A.id1 = 1
      """.stripMargin)
//    while (rs.next()) {
//      println(rs.getString("p"))
//    }
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