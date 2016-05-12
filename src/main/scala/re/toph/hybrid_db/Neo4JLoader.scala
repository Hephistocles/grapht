package re.toph.hybrid_db

import java.io.FileReader

import org.neo4j.graphdb.index.Index
import org.neo4j.graphdb.{Direction, Node, GraphDatabaseService, RelationshipType}

import scala.collection.JavaConversions._
/**
  * Created by christoph on 11/05/16.
  */
object Neo4JLoader {

  case object ROAD extends RelationshipType {
    override def name(): String = "Road"
  }


  def tx[R](block: GraphDatabaseService => R)(implicit graphDb: GraphDatabaseService): R = {
    val tx = graphDb.beginTx()
    try {
      val result = block(graphDb)
      tx.success()
      result
    } finally {
      tx.close()
    }
  }

  def injectnodes(rows: Iterator[(Long, Long, Long)])(implicit graphDb: GraphDatabaseService): Unit = {
    val junctionIndex = tx(db => db.index().forNodes("junctions"))

    rows.grouped(1000).zipWithIndex.foreach {
      case (batch, i) =>
        tx { db =>
          batch.foreach({
            case (id, lat, long) =>
              val node = db.createNode()
              node.setProperty("id", id)
              node.setProperty("lat", lat)
              node.setProperty("long", long)
              junctionIndex.add(node, "id", id)
          })
        }
        println(s"Done batch $i of nodes")
    }
  }


  def injectedges(edges: Iterator[(Long, Long, Long)])(implicit graphDb: GraphDatabaseService): Unit = {
    val junctionIndex: Index[Node] = tx(db => db.index().forNodes("junctions"))

    edges.grouped(1000).zipWithIndex.foreach({
      case (batch, i) =>
        tx { db =>
          batch.foreach({
            case (from, to, distance) =>
              val firstNode = junctionIndex.get("id", from).getSingle()
              val secondNode = junctionIndex.get("id", to).getSingle()
              firstNode.createRelationshipTo(secondNode, ROAD).setProperty("distance", distance)
          })
        }
        println(s"Done batch $i of edges")
    }
    )
  }

  def injectdata(nodeFile:String, edgeFile:String)(implicit graphDb: GraphDatabaseService): Unit = {
    val nodes = new java.io.BufferedReader(new FileReader(nodeFile))
      .lines().iterator()
      .map(_.split(" "))
      .filter(_ (0) == "v")
      .map((ss: Array[String]) => (ss(1).toLong, ss(2).toLong, ss(3).toLong))

    val roads = new java.io.BufferedReader(new FileReader(edgeFile))
      .lines().iterator()
      .map(_.split(" "))
      .filter(_ (0) == "a")
      .map((ss: Array[String]) => (ss(1).toLong, ss(2).toLong, ss(3).toLong))

    injectnodes(nodes)
    injectedges(roads)
  }

  def test()(implicit graphDb: GraphDatabaseService): Unit = {
    tx { db =>
      val junctionIndex =  db.index().forNodes("junctions")
      val start = junctionIndex.get("id", 1).getSingle()
      printf("This node has %d edges\n", start.getDegree(ROAD, Direction.OUTGOING))
      start.getRelationships(ROAD, Direction.OUTGOING).iterator().foreach({
        r => {
          println("One edge: \n" +
            r.getEndNode().getProperty("id")
          )
        }
      })

    }
  }

}
