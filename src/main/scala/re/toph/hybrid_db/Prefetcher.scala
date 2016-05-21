package re.toph.hybrid_db

import java.sql.{Connection, DriverManager, ResultSet}

import anorm.Row

/**
  * Created by christoph on 28/04/16.
  */
trait Prefetcher {
  val driver = "org.postgresql.Driver"
  val url    = "jdbc:postgresql://localhost/christoph"
  val user   = "christoph"
  val pass   = "LockDown1"
  var callCount = 0
  var fetchCount = 0

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

  def get(k:Long) = {
    callCount += 1
    val res = innerGet(k)
    fetchCount += res._2.length
    res
  }

  // To be implemented by sub-classes
  def innerGet(k: Long) : (GraphNode, List[GraphNode])
}
