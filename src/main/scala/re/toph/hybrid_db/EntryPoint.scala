package re.toph.hybrid_db

import java.io.File
import java.sql.{Connection, DriverManager}

import anorm.SQL
import org.anormcypher._
import org.neo4j.graphalgo.{CommonEvaluators, EstimateEvaluator, GraphAlgoFactory}
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import play.api.libs.ws._
import re.toph.hybrid_db.Neo4JLoader.ROAD

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
//			Neo4JLoader.injectdata("points.co", "roads.gr")

//			separator("Closest Points")
//			ClosestPointsPath.go()
//			Timer.printResults(depth=2)
//			Timer.clearTimes()

//			separator("friend of friend search")
//			FriendOfFriend.go()
//			Timer.printResults(depth=2)
//			Timer.clearTimes()

//			separator("A* Search")
//			AStar.go()
//			Timer.printResults(depth=2)
//			Timer.clearTimes()

			// prefetcherTest()
			graphTest()
		} catch {
			case e: Throwable => e.printStackTrace()
		} finally {
			wsclient.close()
			graphDb.shutdown()
		}

	}


	def prefetcherTest(): Unit = {

	var skipFromStart = 0

		val prefetchers =
			(1 to 10)
				.map(b => (s"CTE Lookahead", b, new LookaheadCTEPrefetcher(b))) ++
			(1 to 100 by 10)
					.map(b => (s"Lookahead", b, new LookaheadMultiPrefetcher(b))) ++
			List(("Null Prefetcher", 0, new NullPrefetcher())) ++
			List(1,10,50,100,250,500,1000,5000,10000,20000)
					.map(b => (s"Block", b, new LookaheadBlockPrefetcher(b)))

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
	.drop(2)
		// .take(1)

		val tests =
			routes.flatMap {
				case (start, end) =>
					prefetchers.map {
							case (name, size, p) =>
								() => {
									// now would be a good time to GC. During the test, not so much.
									val g = new Graph(p)
									val astar = new ASFAWEF(g)
									System.gc()
									val (time, _) = Timer.timeWithResult(s"$name,$start,$end", {
										astar.find(start, end)
									})
									printf("%s	%d	%d	%d	%d	%d	%d	%d	%d	", name, size, start, end, g.callCount, g.evictionCount, g.hitCount, g.missCount, time.time, time.subs("DB").time, time.subs("DB").count)
									time.subs.get("DB") match {
										case Some(x) => printf("%d	%d\n",x.time,x.count)
										case None => printf("0	0\n")
									}
									Timer.clearTimes()
								}
						}
			}

		// println("Prefetcher	Size	Start	End	Vertices Requested	Vertices Evicted	Cache Hits	Cache Misses	Total Runtime	Total DB time	DB calls")

		// assuming 10 iterations
		for (_ <- 1 to 1) {
			tests.foreach (f =>{
		
				if (skipFromStart > 0) {
					skipFromStart -= 1
				} else {
					f()
				}
			})
		}
	}






	def graphTest()(implicit db: GraphDatabaseService, connection:Connection): Unit = {

		val RADIUS = 6371
		def heuristic(latitude1:Long, longitude1:Long, latitude2:Long, longitude2:Long ) : Double =
		{
			val (lat1, lng1, lat2, lng2) = (
				Math.toRadians(latitude1/1000000.0),
				Math.toRadians(longitude1/1000000.0),
				Math.toRadians(latitude2/1000000.0),
				Math.toRadians(longitude2/1000000.0))

			val cLa1 = Math.cos(lat1)
			val A = (
				RADIUS * cLa1 * Math.cos(lng1),
				RADIUS * cLa1 * Math.sin(lng1),
				RADIUS * cLa1 * Math.sin(lat1))

			val cLa2 = Math.cos(lat2)
			val B = (
				RADIUS * cLa2 * Math.cos(lng2),
				RADIUS * cLa2 * Math.sin(lng2),
				RADIUS * cLa2 * Math.sin(lat2))

			val res = Math.sqrt((A._1 - B._1) * (A._1 - B._1) +
				(A._2 - B._2) * (A._2 - B._2) +
				(A._3 - B._3) * (A._3 - B._3))

			res
		}

		def neo(from:Long, to:Long): Unit = {
			val tx = db.beginTx()
			try {

				val junctionIndex =	db.index().forNodes("junctions")
				val start = junctionIndex.get("id", from).getSingle()
				val end = junctionIndex.get("id", to).getSingle()

				val (time, path) = Timer.timeWithResult(s"Neo$from,$to", {
						GraphAlgoFactory.aStar(
						PathExpanders.forTypeAndDirection(ROAD, Direction.OUTGOING),
						CommonEvaluators.doubleCostEvaluator("distance"),
						new EstimateEvaluator[java.lang.Double] {
							override def getCost(node1: Node, node2: Node): java.lang.Double = heuristic(
								node1.getProperty("lat").asInstanceOf[Long],
								node1.getProperty("long").asInstanceOf[Long],
								node2.getProperty("lat").asInstanceOf[Long],
								node2.getProperty("long").asInstanceOf[Long]
							)
						}
					).findSinglePath(start, end)
				})

				printf("Neo4J 	%d	%d	%d\n", to, path.weight().toLong, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}

		def grapht(start:Long, end:Long) : Unit = {
			val astar = new ASFAWEF(new Graph(new LookaheadMultiPrefetcher(50)))
				val (time, res) = Timer.timeWithResult(s"Grapht,$start,$end", {
					astar.find(start, end)
				})
				printf("Grapht	%d	%d	%d\n", end, res.dist.toLong, time.time)
				Timer.clearTimes()
		}
		def psql(from:Long, to: Long)(implicit connection: Connection): Unit = {
			val (time, rs) = Timer.timeWithResult(s"Grapht,$from,$to", {
				SQL(
					"SELECT distance::bigint FROM (SELECT (astarroute({from}, {to})).*) t;")
					.on("from" -> from, "to" -> to)()
					.map((row) => {
						row[Long]("distance")
					}).head
			})
			printf("PostgreSQL	%d	%d	%d\n", to, rs, time.time)
		}



		var skipFromStart = 0
		val routes = List(
			(1, 50),
			(171677, 164352),
			(132308, 20756),
			(70188, 151443),
			(54469, 231496),
			(66089, 30814),
			(176648, 126808),
			(58197, 4362)
		).take(4)

		for (_ <- 1 to 10) {
			// skip PSQL to begin with because it is soooo slow!
      routes.foreach {
        case (start, end) =>
					System.gc()
					neo(start, end)
					System.gc()
					grapht(start, end)
			}
			routes.foreach {
				case (start, end) =>
					System.gc()
					psql(start, end)
			}
		}

	}

}
