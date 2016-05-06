package re.toph.hybrid_db

import java.sql.ResultSet
import java.util

/**
  * Created by christoph on 28/04/16.
  */
case class GraphNode(id: Long, edges: List[Edge], properties:Map[String, Any]) {

  override def toString() = {
    var endpoints = ""
    var sep = ""
    edges.foreach (e => {endpoints+=sep + e.to
      sep=", "})
    "(" + id + " " + properties + ") -> " + endpoints
  }
}