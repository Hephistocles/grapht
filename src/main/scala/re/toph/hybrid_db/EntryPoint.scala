package re.toph.hybrid_db

import java.io.File
import java.sql.{Connection, DriverManager}

import anorm.SQL
import org.anormcypher._
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import play.api.libs.ws._
import re.toph.hybrid_db.Neo4JLoader.ROAD

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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

//			AStar.get100(new LookaheadMultiPrefetcher(50))
			 prefetcherTest()
//			graphTest()
//			relaTest()
//			hybridTest()
//			threeSumTest()
//			colinear()
		} catch {
			case e: Throwable => e.printStackTrace()
		} finally {
			wsclient.close()
			graphDb.shutdown()
		}

	}

	val RADIUS = 6371
	def latlngdist(latitude1:Long, longitude1:Long, latitude2:Long, longitude2:Long ) : Double =
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

	def prefetcherTest(): Unit = {

	var skipFromStart = 0

		val prefetchers =
			(1 to 10)
				.map(b => (s"CTE Lookahead", b, new LookaheadCTEPrefetcher(b))) ++
			(1 to 150 by 15)
					.map(b => (s"Lookahead", b, new LookaheadMultiPrefetcher(b))) ++
			List(("Null Prefetcher", 0, new NullPrefetcher())) ++
			List(1,10,50,100,250,500,1000,5000,10000,20000, 50000, 100000)
					.map(b => (s"Block", b, new LookaheadBlockPrefetcher(b)))


    val tx = graphDb.beginTx()
    var routes = List[(Long, Long)]()
    try {
      routes = AStar.findRoutes(50,60)
        .take(3)
        .toList
    } finally {
      tx.close()
    }

//  val routes = List(
//			(1, 50),
//			(171677, 164352),
//			(132308, 20756),
//			(70188, 151443),
//			(54469, 231496),
//			(66089, 30814),
//			(176648, 126808),
//			(58197, 4362)
//		)
//	.drop(2)
		// .take(1)

		val tests =
			routes.flatMap {
				case (start, end) =>
					prefetchers.map {
							case (name, size, p) =>
								() => {
									// now would be a good time to GC. During the test, not so much.
									val g = new Graph(new LookaheadMultiPrefetcher(50))
									val adaptor = new GraphtAdaptor(g)
									val astar = new AStarCalculator(adaptor)
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
		for (_ <- 1 to 3) {
			tests.foreach (f =>{
		
				if (skipFromStart > 0) {
					skipFromStart -= 1
				} else {
					f()
				}
			})
		}
	}




	def hybridTest()(implicit connection:Connection) : Unit = {

		def grapht(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
			val adaptor = new GraphtAdaptor(new Graph(new LookaheadMultiPrefetcher(40)))
			val astar = new AStarCalculator(adaptor)

			Timer.clearTimes()
				val (time, res) = Timer.timeWithResult(s"Grapht", {

					val points = Timer.time("relational", { SQL(
						"""
            	|WITH test
              |AS (
              |	SELECT *
              |		FROM points
              | 	WHERE
              |  		{minLat} < lat AND lat < {maxLat} AND
              |    {minLong} < lng AND lng < {maxLong}
              |)
							| SELECT a.id, b.id, c.id
							| FROM test a, test b, test c
							| WHERE (b.lng-a.lng)*(c.lat-b.lat) = (c.lng-b.lng)*(b.lat-a.lat)
							| 	AND a.id < b.id AND b.id < c.id""".stripMargin)
						.on("minLat"->minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong)()
						.map( row => {
							val r =  row.asList
							(r(0).asInstanceOf[Long], r(1).asInstanceOf[Long], r(2).asInstanceOf[Long])
						})
					.head})

					Timer.time("graph", astar.find(points._1, points._2))

				})
//				printf("Grapht\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.dist.toLong, time.time)
				Timer.printResults()
				Timer.clearTimes()
		}
		def neo2(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
			val tx = db.beginTx()
			try {
				val adaptor = new NeoAdaptor()(db)
				val astar = new AStarCalculator(adaptor)
				val (time, res) = Timer.timeWithResult(s"Neo4J", {

					val finals = Timer.time("relational", {
            val label: Label = Label.label("midset")

            // part one - identify geographically local points
            var group = Map[String, ListBuffer[Long]]()
            db.getAllNodes.asScala.foreach(n => {
              if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < minLong
                && minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {
                val key = n.getProperty("payload").asInstanceOf[String].substring(0,2)
                if (group.contains(key)) group(key) += n.getId
                else group += key -> ListBuffer[Long](n.getId())
              }
            })

            // part three (not performed within Neo) - find groups with several entries
            var finals = ListBuffer[List[Long]]()
            for ((key, ids) <- group) {
              if (ids.length > 1) finals += ids.toList
            }
						finals.toList
					})


					Timer.time("graph", astar.find(finals.head(0), finals.head(1)))
				})
//				printf("Neo4J\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.dist.toLong, time.time)
				Timer.printResults()
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}
		def neo(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
			val tx = db.beginTx()
			try {
				val adaptor = new GraphtAdaptor(new Graph(new LookaheadMultiPrefetcher(50)))
				val astar = new AStarCalculator(adaptor)
				val (time, res) = Timer.timeWithResult(s"Neo4J", {


					// part one - identify geographically local points
					val nodeList = ListBuffer[Node]()
					db.getAllNodes.asScala.foreach(n => {
						if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < minLong
							&& minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {

							nodeList += n
						}
					})

					// part two (not performed within Neo) - find threesum triples!
					val nodeListFinal = nodeList.toList
					var finalsBuffer = ListBuffer[(Long, Long, Long)]()
					for (a <- nodeListFinal) {
						for (b <- nodeListFinal) {
							for (c <- nodeListFinal) {
								// (n−b)(x−m)=(y−n)(m−a)
								if (((b.getProperty("long").asInstanceOf[Long] - a.getProperty("long").asInstanceOf[Long]) *
									(c.getProperty("lat").asInstanceOf[Long] - b.getProperty("lat").asInstanceOf[Long])
									==
									(c.getProperty("long").asInstanceOf[Long] - b.getProperty("long").asInstanceOf[Long]) *
										(b.getProperty("lat").asInstanceOf[Long] - a.getProperty("lat").asInstanceOf[Long])) &&
									 a.getId() < b.getId() && b.getId() < c.getId()
								){
									finalsBuffer += ((a.getId(), b.getId(), c.getId()))
								}
							}
						}
					}


					val finals = finalsBuffer.toList
					astar.find(finals.head._1, finals.head._2)
				})
				printf("Neo4J1\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.dist.toLong, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}


		val bounds = List(
			((-74450000, -73500010), (40301000,40301500))
		)

		println("Engine\tbounds\tresults\ttime")
		for (_ <- 1 to 1) {
			bounds.foreach {
				case ((minLat, maxLat), (minLng, maxLng)) =>
					System.gc()
					neo2(minLat, maxLat, minLng, maxLng)
//					System.gc()
//					neo(minLat, maxLat, minLng, maxLng)
					System.gc()
					grapht(minLat, maxLat, minLng, maxLng)
				//					grapht(start, end)
				//					System.gc()
			}
		}
	}



	def colinear()(implicit connection:Connection) : Unit = {

		def psql(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {

			val tx = db.beginTx()
			try {
				val (time, res) = Timer.timeWithResult(s"Grapht", {
					SQL(
						"""
							|WITH test
							|AS (
							|	SELECT *
							|		FROM points
							| 	WHERE
							|  		{minLat} < lat AND lat < {maxLat} AND
							|    {minLong} < lng AND lng < {maxLong}
							|)
              | SELECT a.id, b.id, c.id
              | FROM test a, test b, test c
              | WHERE (b.lng-a.lng)*(c.lat-b.lat) = (c.lng-b.lng)*(b.lat-a.lat)
              | 	AND a.id<>b.id AND a.id<>c.id AND b.id<>c.id AND a.id < b.id AND b.id < c.id""".stripMargin)
						.on("minLat"->minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong)()
						//						.map( r => {
						//							val r1 = r[List[Int]]("matches")
						//							r1
						//						})
						.length
				})
				printf("Grapht\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res, time.time)
				printf("PostgreSQL\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}

		def neo2(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
			val tx = db.beginTx()
			try {
				val (time, res) = Timer.timeWithResult(s"Neo4J", {

					val label: Label = Label.label("midset")

					// part one - identify geographically local points
					val nodeList = ListBuffer[Node]()
					db.getAllNodes.asScala.foreach(n => {
						if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < minLong
							&& minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {

							nodeList += n
						}
					})

					// part two (not performed within Neo) - find threesum triples!
					val edgeListFinal = nodeList.toList
					var count = 0
					for (a <- edgeListFinal) {
						for (b <- edgeListFinal) {
							for (c <- edgeListFinal) {
								// (n−b)(x−m)=(y−n)(m−a)
								if (((b.getProperty("long").asInstanceOf[Long] - a.getProperty("long").asInstanceOf[Long]) *
									(c.getProperty("lat").asInstanceOf[Long] - b.getProperty("lat").asInstanceOf[Long])
									==
									(c.getProperty("long").asInstanceOf[Long] - b.getProperty("long").asInstanceOf[Long]) *
										(b.getProperty("lat").asInstanceOf[Long] - a.getProperty("lat").asInstanceOf[Long])) &&
									a.getId != b.getId() && a.getId() != c.getId() && b.getId() != c.getId() && a.getId() < b.getId() && b.getId() < c.getId()
								){
									count += 1
								}
							}
						}
					}

					count
				})
				printf("Neo4J\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}

		// labelled version - commented out because I can't apply labels to edges!



		//		def neo(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
		//			val tx = db.beginTx()
		//			try {
		//				val (time, res) = Timer.timeWithResult(s"Neo4J", {
		//
		//					val label: Label = Label.label("midset")
		//
		//					// part one - identify geographically local points
		//					db.getAllNodes.asScala.foreach(n => {
		//						if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < maxLat
		//							&& minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {
		//							// apply label  to edges so we can find it again
		//							n.getRelationships(ROAD, Direction.OUTGOING).asScala.foreach(r => r.addLabel(label))
		//						}
		//					})
		//
		//
		//					// part two - group by substring (and remove label)
		//					var group = Map[String, ListBuffer[Long]]()
		//					db.findNodes(label).asScala.foreach(n=> {
		//						val key = n.getProperty("payload").asInstanceOf[String].substring(0,2)
		//						if (group.contains(key)) group(key) += n.getId
		//						else group += key -> ListBuffer[Long](n.getId())
		//						n.removeLabel(label)
		//					})
		//
		//					// part three (not performed within Neo) - find groups with several entries
		//					var finals = ListBuffer[List[Long]]()
		//					for ((key, ids) <- group) {
		//						if (ids.length > 1) finals += ids.toList
		//					}
		//
		//					finals.toList
		//				})
		//				printf("Neo4J1\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.length, time.time)
		//				Timer.clearTimes()
		//			} finally {
		//				tx.close()
		//			}
		//		}


		val bounds = List(
			((-74450000, -73500010), (40301000,40301500))
		)

		println("Engine\tbounds\tresults\ttime")
		for (_ <- 1 to 10) {
			bounds.foreach {
				case ((minLat, maxLat), (minLng, maxLng)) =>
					System.gc()
					neo2(minLat, maxLat, minLng, maxLng)
					//					System.gc()
					//					neo(minLat, maxLat, minLng, maxLng, 5534)
					System.gc()
					psql(minLat, maxLat, minLng, maxLng)
				//					grapht(start, end)
				//					System.gc()
			}
		}
	}

	def threeSumTest()(implicit connection:Connection) : Unit = {

		def psql(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long, target:Long)(implicit db : GraphDatabaseService) : Unit = {

			val tx = db.beginTx()
			try {
				val (time, res) = Timer.timeWithResult(s"Grapht", {
					SQL(
						"""
							|WITH test
							|AS (
							|	SELECT points.id, edges.id eid, edges.dist
							|		FROM edges JOIN points ON edges.id1=points.id
							| 	WHERE
							|  		{minLat} < lat AND lat < {maxLat} AND
							|    {minLong} < lng AND lng < {maxLong}
							|)
 							| SELECT a.id a, b.id b, c.id c, a.dist + b.dist + c.dist tot
 							| FROM test a, test b, test c
							| WHERE a.eid<>b.eid AND b.eid<>c.eid AND a.eid<>c.eid
							| AND a.dist+b.dist+c.dist={target};""".stripMargin)
						.on("minLat"->minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong, "target" -> target)()
//						.map( r => {
//							val r1 = r[List[Int]]("matches")
//							r1
//						})
						.length
				})
				printf("Grapht\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res, time.time)
				printf("PostgreSQL\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}

		def neo2(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long, target:Long)(implicit db : GraphDatabaseService) : Unit = {
			val tx = db.beginTx()
			try {
				val (time, res) = Timer.timeWithResult(s"Neo4J", {

					val label: Label = Label.label("midset")

					// part one - identify geographically local points
					val edgeList = ListBuffer[Relationship]()
					db.getAllNodes.asScala.foreach(n => {
						if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < minLong
							&& minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {

							n.getRelationships(ROAD, Direction.OUTGOING).asScala.foreach(r => {edgeList += r})
						}
					})

					// part two (not performed within Neo) - find threesum triples!
					val edgeListFinal = edgeList.toList
					var count = 0
					for (a <- edgeListFinal) {
						for (b <- edgeListFinal) {
							for (c <- edgeListFinal) {
								if (a.getProperty("distance").asInstanceOf[Long] + b.getProperty("distance").asInstanceOf[Long] + c.getProperty("distance").asInstanceOf[Long]
												== target) {
									count += 1
								}
							}
						}
					}

					count
				})
				printf("Neo4J\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}

		// labelled version - commented out because I can't apply labels to edges!



//		def neo(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
//			val tx = db.beginTx()
//			try {
//				val (time, res) = Timer.timeWithResult(s"Neo4J", {
//
//					val label: Label = Label.label("midset")
//
//					// part one - identify geographically local points
//					db.getAllNodes.asScala.foreach(n => {
//						if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < maxLat
//							&& minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {
//							// apply label  to edges so we can find it again
//							n.getRelationships(ROAD, Direction.OUTGOING).asScala.foreach(r => r.addLabel(label))
//						}
//					})
//
//
//					// part two - group by substring (and remove label)
//					var group = Map[String, ListBuffer[Long]]()
//					db.findNodes(label).asScala.foreach(n=> {
//						val key = n.getProperty("payload").asInstanceOf[String].substring(0,2)
//						if (group.contains(key)) group(key) += n.getId
//						else group += key -> ListBuffer[Long](n.getId())
//						n.removeLabel(label)
//					})
//
//					// part three (not performed within Neo) - find groups with several entries
//					var finals = ListBuffer[List[Long]]()
//					for ((key, ids) <- group) {
//						if (ids.length > 1) finals += ids.toList
//					}
//
//					finals.toList
//				})
//				printf("Neo4J1\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.length, time.time)
//				Timer.clearTimes()
//			} finally {
//				tx.close()
//			}
//		}


		val bounds = List(
			((-74450000, -73500010), (40301000,40301500))
		)

		println("Engine\tbounds\tresults\ttime")
		for (_ <- 1 to 10) {
			bounds.foreach {
				case ((minLat, maxLat), (minLng, maxLng)) =>
					System.gc()
					neo2(minLat, maxLat, minLng, maxLng, 5534)
//					System.gc()
//					neo(minLat, maxLat, minLng, maxLng, 5534)
					System.gc()
					psql(minLat, maxLat, minLng, maxLng, 5534)
				//					grapht(start, end)
				//					System.gc()
			}
		}
	}
	def relaTest()(implicit connection:Connection) : Unit = {

		def psql(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {

			val tx = db.beginTx()
			try {
				val (time, res) = Timer.timeWithResult(s"Grapht", {
					SQL(
            """
              |WITH test
              |AS (
              |	SELECT * FROM points
              | 	WHERE
              |  		{minLat} < lat AND lat < {maxLat} AND
              |    {minLong} < lng AND lng < {maxLong}
              |)
              |	 SELECT
              |  		array_agg(id) matches
              |  FROM test
              |  GROUP BY substr(payload,1,2)
              |  HAVING count(*)>1;""".stripMargin)
						.on("minLat"->minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong)()
						.map( r => {
								val r1 = r[List[Int]]("matches")
							r1
						})
				})
				printf("Grapht\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.length, time.time)
				printf("PSQL\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.length, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}
		def neo2(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
			val tx = db.beginTx()
			try {
				val (time, res) = Timer.timeWithResult(s"Neo4J", {

					val label: Label = Label.label("midset")

					// part one - identify geographically local points
					var group = Map[String, ListBuffer[Long]]()
					db.getAllNodes.asScala.foreach(n => {
						if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < minLong
							&& minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {
							val key = n.getProperty("payload").asInstanceOf[String].substring(0,2)
							if (group.contains(key)) group(key) += n.getId
							else group += key -> ListBuffer[Long](n.getId())
						}
					})

					// part three (not performed within Neo) - find groups with several entries
					var finals = ListBuffer[List[Long]]()
					for ((key, ids) <- group) {
						if (ids.length > 1) finals += ids.toList
					}

					finals.toList
				})
				printf("Neo4J\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.length, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}
		def neo(minLat:Long, maxLat:Long, minLong:Long, maxLong:Long)(implicit db : GraphDatabaseService) : Unit = {
			val tx = db.beginTx()
			try {
				val (time, res) = Timer.timeWithResult(s"Neo4J", {

					val label: Label = Label.label("midset")

					// part one - identify geographically local points
					db.getAllNodes.asScala.foreach(n => {
					if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < maxLat
						&& minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {
						// apply label so we can find it again
							n.addLabel(label)
						}
					})


					// part two - group by substring (and remove label)
					var group = Map[String, ListBuffer[Long]]()
					db.findNodes(label).asScala.foreach(n=> {
						val key = n.getProperty("payload").asInstanceOf[String].substring(0,2)
						if (group.contains(key)) group(key) += n.getId
						else group += key -> ListBuffer[Long](n.getId())
						n.removeLabel(label)
					})

					// part three (not performed within Neo) - find groups with several entries
					var finals = ListBuffer[List[Long]]()
					for ((key, ids) <- group) {
						if (ids.length > 1) finals += ids.toList
					}

					finals.toList
				})
				printf("Neo4J1\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.length, time.time)
				Timer.clearTimes()
			} finally {
				tx.close()
			}
		}


		val bounds = List(
			((-74450000, -73500010), (40301000,40301500))
		)

		println("Engine\tbounds\tresults\ttime")
		for (_ <- 1 to 10) {
			bounds.foreach {
				case ((minLat, maxLat), (minLng, maxLng)) =>
					System.gc()
					neo2(minLat, maxLat, minLng, maxLng)
					System.gc()
					neo(minLat, maxLat, minLng, maxLng)
					System.gc()
					psql(minLat, maxLat, minLng, maxLng)
//					grapht(start, end)
//					System.gc()
			}
		}
	}



	def graphTest()(implicit  connection:Connection): Unit = {

		def neo2(start:Long, end:Long)(implicit db : GraphDatabaseService) : Unit = {
			val tx = db.beginTx()
			try {
        val adaptor = new NeoAdaptor()(db)
        val astar = new AStarCalculator(adaptor)
        val (time, res) = Timer.timeWithResult(s"Grapht,$start,$end", {
          astar.find(start, end)
        })
        printf("neo2	%d	%d	%d	%.3f\n", end, res.dist.toLong,res.length, time.time/1.0e9)
        Timer.clearTimes()
			} finally {
				tx.close()
			}
		}
		def grapht(start:Long, end:Long) : Unit = {
			val adaptor = new GraphtAdaptor(new Graph(new LookaheadMultiPrefetcher(50)))
			val astar = new AStarCalculator(adaptor)
			val (time, res) = Timer.timeWithResult(s"Grapht,$start,$end", {
				astar.find(start, end)
			})
			printf("Grapht	%d	%d	%d	%.3f\n", end, res.dist.toLong, res.length, time.time/1.0e9)
			Timer.clearTimes()
		}
		def psql2(start:Long, end:Long) : Unit = {
			val adaptor = new GraphtAdaptor(new Graph(new NullPrefetcher()))
			val astar = new AStarCalculator(adaptor)
			val (time, res) = Timer.timeWithResult(s"Grapht,$start,$end", {
				astar.find(start, end)
			})
			printf("PSQL	%d	%d	%.3f\n", end, res.dist.toLong, time.time/1.0e9)
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
			printf("PostgreSQL	%d	%d	%.3f\n", to, rs, time.time/1.0e9)
		}



//		val tx = graphDb.beginTx()
//		var routes = List[(Long, Long)]()
//		try {
//			routes = AStar.findRoutes(50,60)
//				.take(10)
//				.toList
//		} finally {
//			tx.close()
//		}
//		println(routes)

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

		routes.foreach {
			case (start, end) =>
			for (_ <- 1 to 10) {
					// skip PSQL to begin with because it is soooo slow!
					System.gc()
					neo2(start, end)
					System.gc()
					grapht(start, end)
//					System.gc()
//					psql2(start, end)
			}
				println()
//			routes.foreach {
//				case (start, end) =>
//					System.gc()
//					psql(start, end)
//			}
		}

	}

}
