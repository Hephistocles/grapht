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
object HelloWorld {

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

  def main(args: Array[String]): Unit = {

    try {

      // uncomment this to load data from "points.co" and "roads.gr" into neo4j
//      Neo4JLoader.injectdata("points.co", "roads.gr")

      FriendOfFriend.go()
      Timer.printResults(depth=1)
      Timer.clearTimes()
//
//        for (it <- 1 to 3) {
//          println(
//              Timer.time("Neo", {
//              getHopDistsNeo(hops, id)
//            }, false)
//            .length + " paths found for Neo")
//
//          println(
//          Timer.time("Cypher", {
//            getHopDistsCypher(hops, id)
//          }, false)
//            .length + " paths found for Cypher")
//          //          .foreach(println)
//
//          println(
//            Timer.time("SQL CTE", {
//              getHopDistsCTESQL(hops, id)
//            }, false)
//              .length + " paths found for SQL CTE")
//          //          .foreach(println)
//
//            println(
//          Timer.time("SQL UNIONS", {
//            getHopDistsSQL(hops, id)
//          }, false)
//            .length + " paths found for SQL UNIONS")
//          //          .foreach(println)
//
//          List(5000, 10000, 20000
//            // for some reason 100,000+ causes a stackoverflow somewhere
//            //          ,100000
//          ).foreach(i =>
//            println(
//            Timer.time("Block " + i, {
//              getHopDists(hops, id, new LookaheadBlockPrefetcher(i))
//            }, false)
//              .length + " paths found for Block " + i)
//            //            .foreach(println)
//          )
//
//              println(
//          Timer.time("Lookahead Join", {
//            getHopDists(hops, id, new LookaheadJoinPrefetcher(3))
//          }, false)
//            .length + " paths found for Lookahead")
////            .foreach(println)
//
//          for (i <- 3 to 5) {
//            println(
//            Timer.time(s"Lookahead Multi ($i)", {
//              getHopDists(hops, id, new LookaheadMultiPrefetcher(i))
//            }, false)
//              .length + s" paths found for Lookahead Multi ($i)")
//            //            .foreach(println)
//          }
//        }
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      wsclient.close()
      graphDb.shutdown()
      Timer.printResults()
    }

  }

}