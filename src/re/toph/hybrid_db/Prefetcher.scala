package re.toph.hybrid_db

import java.sql.{ResultSet, Connection, DriverManager}
import java.util

/**
  * Created by christoph on 28/04/16.
  */
trait Prefetcher {
  val driver = "org.postgresql.Driver"
  val url    = "jdbc:postgresql://localhost/christoph"
  val user   = "christoph"
  val pass   = "LockDown1"

  Class.forName(driver)
  var connection:Connection =  DriverManager.getConnection(url, user, pass)

  def getMap(resultSet:ResultSet): util.HashMap[String, String] = {
    val map = new util.HashMap[String, String]()
    map.put("id", resultSet.getString("id"))
//    val meta = resultSet.getMetaData
//    for (i <- 1 to meta.getColumnCount) {
//      map.put(
//        meta.getColumnName(i),
//        resultSet.getString(i)
//      )
//    }
    map
  }
  // To be implemented by sub-classes
  def get(k: Int) : (GraphNode, List[GraphNode])
}
