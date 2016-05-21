package re.toph.hybrid_db

import java.io.File
import java.sql.{Connection, DriverManager}

import org.anormcypher._
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import play.api.libs.ws._

/**
  * Created by christoph on 28/04/16.
  */
object EntryPoint {

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost/christoph"
  val user = "christoph"
  val pass = "LockDown1"

  Class.forName(driver)
  implicit val connection: Connection = DriverManager.getConnection(url, user, pass)
  implicit val wsclient = ning.NingWSClient()
  implicit val connection2 = Neo4jREST("localhost", 7474, "neo4j", pass)
  implicit val ec = scala.concurrent.ExecutionContext.global
  implicit val graphDb: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File("hdb-compare"))

  def separator(msg:String): Unit = {
    println(
      s"""
         |
         |
         |**${"*"* msg.length}**
         |* ${msg.toUpperCase} *
         |**${"*"* msg.length}**
         | """.stripMargin)
  }

  def main(args: Array[String]): Unit = {

    try {

      // uncomment this to load data into neo4j
//      Neo4JLoader.injectdata("points.co", "roads.gr")

//      separator("Closest Points")
//      ClosestPointsPath.go()
//      Timer.printResults(depth=2)
//      Timer.clearTimes()

//      separator("friend of friend search")
//      FriendOfFriend.go()
//      Timer.printResults(depth=2)
//      Timer.clearTimes()

//      separator("A* Search")
//      AStar.go()
//      Timer.printResults(depth=2)
//      Timer.clearTimes()
      prefetcherTest()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      wsclient.close()
      graphDb.shutdown()
    }

  }


  def prefetcherTest(): Unit = {

    val prefetchers =
      List(("Null Prefetcher", new NullPrefetcher())) ++
      (40 to 50 by 5)
          .map(b => (s"Lookahead ($b)", new LookaheadMultiPrefetcher(b))) ++
      List(10000, 20000)
          .map(b => (s"Block ($b)", new LookaheadBlockPrefetcher(b))) ++
      (1 to 5 by 1)
        .map(b => (s"CTE Lookahead ($b)", new LookaheadCTEPrefetcher(b)))

    val routes = List(
      (1, 50),
      (171677, 164352),
      (132308, 20756),
      (70188, 151443),
      (54469, 231496),
      (66089, 30814),
      (176648, 126808),
      (58197, 4362)
    )
    .take(1)

    val tests =
      routes.flatMap {
        case (start, end) =>
          prefetchers.map {
              case (s, p) =>
                () => {
                  val g = new Graph(p)
                  val astar = new ASFAWEF(g)
                  val (time, _) = Timer.timeWithResult(s"$s,$start,$end", {
                    astar.find(start, end)
                  })
                  printf("%s,%d,%d,%d,%d,%d,%d,%d,", s, start, end, g.callCount, g.evictionCount, g.hitCount, g.missCount, time.time, time.subs("DB").time, time.subs("DB").count)
                  time.subs.get("DB") match {
                    case Some(x) => printf("%d,%d\n",x.time,x.count)
                    case None => printf("0,0\n")
                  }
                  Timer.clearTimes()
                }
            }
      }

    println("Prefetcher,Start,End,Vertices Requested,Vertices Evicted,Cache Hits, Cache Misses,Total Runtime,Total DB time,DB calls")
    for (_ <- 1 to 3) {
      tests.foreach (f => f())
    }
    println("All over")
  }


}