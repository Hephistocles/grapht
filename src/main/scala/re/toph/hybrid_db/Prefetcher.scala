package re.toph.hybrid_db

import java.sql.{ResultSet, Connection, DriverManager}
import java.util

import anorm.Row

/**
  * Created by christoph on 28/04/16.
  */
trait Prefetcher {
  val driver = "org.postgresql.Driver"
  val url    = "jdbc:postgresql://localhost/christoph"
  val user   = "christoph"
  val pass   = "LockDown1"

  Class.forName(driver)
  val connection:Connection =  DriverManager.getConnection(url, user, pass)

  def getMap(row:Row) = {
    row.asMap
//      .map(a => {
//      val (k,v) = a
//      (k.substring(1), v)
//    })
  }
  def getMap(resultSet:ResultSet) = {
    var map = Map[String, Any]()
    val meta = resultSet.getMetaData
    for (i <- 1 to meta.getColumnCount) {
      map +=
        meta.getColumnName(i) ->
         resultSet.getObject(i)

    }
    map
  }

  // To be implemented by sub-classes
  def get(k: Long) : (GraphNode, List[GraphNode])
}
