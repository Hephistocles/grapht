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

      separator("friend of friend search")
      FriendOfFriend.go()
      Timer.printResults(depth=1)
      Timer.clearTimes()

      separator("A* Search")
      AStar.go()
      Timer.printResults(depth=1)
      Timer.clearTimes()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      wsclient.close()
      graphDb.shutdown()
      Timer.printResults()
    }

  }

}