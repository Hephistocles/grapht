package re.toph.hybrid_db

import java.io.{PrintWriter, File}
import java.sql.{Connection, DriverManager}

import anorm.{SqlParser, SQL}
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

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  Class.forName(driver)
  implicit val connection: Connection = DriverManager.getConnection(url, user, pass)
  implicit val wsclient = ning.NingWSClient()
  implicit val connection2 = Neo4jREST("localhost", 7474, "neo4j", pass)
  implicit val ec = scala.concurrent.ExecutionContext.global
  implicit val graphDb: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File("hdb-compare"))

  def separator(msg: String): Unit = {
    println(
      s"""
         				 |
         				 |
         				 |**${"*" * msg.length}**
         				 |* ${msg.toUpperCase} *
         				 |**${"*" * msg.length}**
         				 | """.stripMargin)
  }
  def getFile(desired:String, ext:String="tsv") :File = {
    val dir = new File("benches")
    if (!dir.exists()) dir.mkdir()

    var count = 0
    for (file <- dir.list()) {
      if (file.startsWith(desired)) count += 1
    }

    val newName = if (count>0) s"$desired-$count" else desired
    new File(s"benches/$newName.$ext")
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
      //			 prefetcherTest()
      //			graphTest()
      //			relaTest()
      //			hybridColinear()
      //			threeSumTst()


      separator("Relational")
      printToFile(getFile("rela-leven")) { p =>
        relaLevenshtein(p, 3, 10)
      }

      separator("Hybrid")
      printToFile(getFile("hybrid-astar-leven")) { p =>
        hybridPOI(p, 3, 10)
      }

      separator("Graph")
      printToFile(getFile("graph-astar")) { p =>
        graphTest(p, 3, 10)
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      wsclient.close()
      graphDb.shutdown()
    }

  }

  // helpers
  val RADIUS = 6371
  def md5(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }
  def minimum(i1: Int, i2: Int, i3: Int) = Math.min(Math.min(i1, i2), i3)
  def levenshtein(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

    for (j <- 1 to s2.length; i <- 1 to s1.length)
      dist(j)(i) = if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }
  def latlngdist(latitude1: Long, longitude1: Long, latitude2: Long, longitude2: Long): Double = {

    val (lat1, lng1, lat2, lng2) = (
      Math.toRadians(latitude1 / 1000000.0),
      Math.toRadians(longitude1 / 1000000.0),
      Math.toRadians(latitude2 / 1000000.0),
      Math.toRadians(longitude2 / 1000000.0))

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




  def prefetcherTest(p: PrintWriter, routesNum:Int, iterations:Int): Unit = {

    val prefetchers =
      (1 to 10)
        .map(b => (s"CTE Lookahead", b, new LookaheadCTEPrefetcher(b))) ++
        (1 to 150 by 15)
          .map(b => (s"Lookahead", b, new LookaheadMultiPrefetcher(b))) ++
        List(("Null Prefetcher", 0, new NullPrefetcher())) ++
        List(1, 10, 50, 100, 250, 500, 1000, 5000, 10000, 20000, 50000, 100000)
          .map(b => (s"Block", b, new LookaheadBlockPrefetcher(b)))


    val tx = graphDb.beginTx()
    var routes = List[(Long, Long)]()
    try {
      routes = AStar.findRoutes(50, 60)
        .take(routesNum)
        .toList
    } finally {
      tx.close()
    }

    val tests =
      routes.flatMap {
        case (start, end) =>
          prefetchers.map {
            case (name, size, prefetcher) =>
              () => {
                // now would be a good time to GC. During the test, not so much.
                val g = new Graph(prefetcher)
                val adaptor = new GraphtAdaptor(g)
                val astar = new AStarCalculator(adaptor)
                System.gc()
                val (time, _) = Timer.timeWithResult(s"$name,$start,$end", {
                  astar.find(start, end)
                })
                p.printf(f"$name%s	$size%d	$start%d	$end%d	${g.callCount}%d	${g.evictionCount}%d	${g.hitCount}%d"
                         + f"	${g.missCount}%d	${time.time}%d")
                time.subs.get("DB") match {
                  case Some(x) => p.printf(f"${x.time}%d	${x.count}%d\n")
                  case None => p.printf("0	0\n")
                }
                Timer.clearTimes()
              }
          }
      }

    // p.println("Prefetcher	Size	Start	End	Vertices Requested	Vertices Evicted	Cache Hits	Cache Misses	Total Runtime	Total DB time	DB calls")

    // assuming 10 iterations
    for (_ <- 1 to iterations) {
      tests.foreach(f => f())
    }
  }

  def hybridPOI(p: PrintWriter, routesNum: Int, iterations: Int)(implicit connection: Connection): Unit = {

    def psql(startId: Long, endId: Long, queries: List[String])(implicit db: GraphDatabaseService): Unit = {
      val adaptor = new PSQLAdaptor("points", "id", "edges", "id1", "id2")
      val astar = new AStarCalculator(adaptor)

      val (time, res) = Timer.timeWithResult(s"PSQL", {

        // step one - find a route
        val vertices: Seq[Long] = Timer.time("graph", {
          astar.find(startId, endId).vertices
        })

        Timer.time("relational", {
          val ids = ListBuffer[Long]()
          for (q <- queries) {
            Timer.time("query",
              SQL("SELECT *, levenshtein({query}, payload) relevance FROM points ORDER BY relevance, id LIMIT 1")
                .on("query" -> q)()
                .foreach(r => ids += r[Long]("id"))
            )
          }

          ids.toList
        })
      })
      p.println(f"PostgreSQL\t$startId%d,$endId%d\t${res.toString}%s\t${time.time}%d\t${time.subs("graph").time}%d\t${time.subs("relational").time}%d")
      Timer.clearTimes()
    }
    def grapht(startId: Long, endId: Long, queries: List[String])(implicit db: GraphDatabaseService): Unit = {
      val adaptor = new GraphtAdaptor(new Graph(new LookaheadMultiPrefetcher(40)))
      val astar = new AStarCalculator(adaptor)

      val (time, res) = Timer.timeWithResult(s"Grapht", {

        // step one - find a route
        val vertices: Seq[Long] = Timer.time("graph", {
          astar.find(startId, endId).vertices
        })

        // step three query within bounds
        Timer.time("relational", {
          val ids = ListBuffer[Long]()
          for (q <- queries) {
            Timer.time("query",
              SQL("SELECT *, levenshtein({query}, payload) relevance FROM points ORDER BY relevance, id LIMIT 1")
                .on("query" -> q)()
                .foreach(r => ids += r[Long]("id"))
            )
          }

          ids.toList
        })
      })
      p.println(f"Grapht\t$startId%d,$endId%d\t${res.toString}%s\t${time.time}%d\t${time.subs("graph").time}%d\t${time.subs("relational").time}%d")
      Timer.clearTimes()
    }
    def neo(startId: Long, endId: Long, queries: List[String])(implicit db: GraphDatabaseService): Unit = {
      val tx = db.beginTx()
      try {
        val adaptor = new NeoAdaptor()(db)
        val astar = new AStarCalculator(adaptor)
        val index = db.index().forNodes("junctions")
        val (time, res) = Timer.timeWithResult(s"Neo4J", {


          // part one - find a route
          val path: astar.Path = Timer.time("graph", astar.find(startId, endId))

          val res = Timer.time("relational", {

            val ids = ListBuffer[Long]()
            for (q <- queries) {
              var (score, node): (Int, Node) = (Int.MaxValue, null)
              Timer.time("query", {
                db.getAllNodes.asScala.foreach(n => {
                  //									db.findNodes(label).asScala.foreach(n => {
                  val dist = levenshtein(q, n.getProperty("payload").asInstanceOf[String])
                  if (score > dist) {
                    score = dist
                    node = n
                  } else if (score == dist) {
                    // tie break by id
                    if (node.getProperty("id").asInstanceOf[Long] > n.getProperty("id").asInstanceOf[Long]) {
                      node = n
                    }
                  }
                })
                ids += node.getProperty("id").asInstanceOf[Long]
              })
            }

            ids.toList
          })

          res
        })
        p.println(f"Neo4J\t$startId%d,$endId%d\t${res.toString}%s\t${time.time}%d\t${time.subs("graph").time}%d\t${time.subs("relational").time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }

    val tx = graphDb.beginTx()
    var routes = List[(Long, Long)]()
    try {
      routes = AStar.findRoutes(40, 60)
        .take(routesNum)
        .toList
    } finally {
      tx.close()
    }

    p.println("Engine\tRoute\tResults\tTotal Time\tGraph Time\tRelational Time")
    for (i <- 1 to iterations) {
      println(s"Hybrid iteration $i")
      routes.foreach {
        case (startId, endId) =>
          println(s"    route $startId -> $endId")
          val queries = List(md5(s"$startId$endId"))
          printf("        Doing neo...")
          System.gc()
          neo(startId, endId, queries)
          p.flush()
          printf("Done!\n")
          printf("        Doing sql...")
          System.gc()
          psql(startId, endId, queries)
          p.flush()
          printf("Done!\n")
          printf("        Doing grapht...")
          System.gc()
          grapht(startId, endId, queries)
          p.flush()
          print("Done!\n")
      }
    }
  }

  def hybridColinear(p: PrintWriter, routesNum:Int, iterations:Int)(implicit connection: Connection): Unit = {

    def grapht(startId: Long, endId: Long)(implicit db: GraphDatabaseService): Unit = {
      val adaptor = new GraphtAdaptor(new Graph(new LookaheadMultiPrefetcher(40)))
      val astar = new AStarCalculator(adaptor)

      val (time, res) = Timer.timeWithResult(s"Grapht", {

        val vertices: Seq[Long] = Timer.time("graph", {
          astar.find(startId, endId).vertices
        })
        // prepare the insertion query

        val points = Timer.time("relational", {
          SQL(
            """
              	 						|WITH test
              	 					|AS (
              	 					|	SELECT *
              	 					|		FROM points
              	 					| 	WHERE
              	 					|  id IN ({vertices})
              	 					|)
              | SELECT a.id, b.id, c.id
              | FROM test a, test b, test c
              | WHERE (b.lng-a.lng)*(c.lat-b.lat) = (c.lng-b.lng)*(b.lat-a.lat)
              | 	AND a.id < b.id AND b.id < c.id""".stripMargin)
            .on("vertices" -> vertices)()
            .map(row => {
              val r = row.asList
              (r(0).asInstanceOf[Long], r(1).asInstanceOf[Long], r(2).asInstanceOf[Long])
            })
        })

        points.length
      })
      p.println(f"Grapht\t$startId%d,$endId%d\t${res.toString}%s\t${time.time}%d\t${time.subs("graph").time}%d\t${time.subs("relational").time}%d")
      //			Timer.printResults()
      Timer.clearTimes()
    }

    def neo(startId: Long, endId: Long)(implicit db: GraphDatabaseService): Unit = {
      val tx = db.beginTx()
      try {
        val adaptor = new NeoAdaptor()(db)
        val astar = new AStarCalculator(adaptor)
        val index = db.index().forNodes("junctions")
        val (time, res) = Timer.timeWithResult(s"Neo4J", {


          // part one - find a route
          val path: astar.Path = Timer.time("graph", astar.find(startId, endId))


          // part two (not performed within Neo) - find collinear triples!
          val res = Timer.time("relational", {
            val nodeListFinal = path.vertices.map(l => index.get("id", l).getSingle())
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
                    a.getProperty("id").asInstanceOf[Long] < b.getProperty("id").asInstanceOf[Long] &&
                    b.getProperty("id").asInstanceOf[Long] < c.getProperty("id").asInstanceOf[Long]
                  ) {
                    finalsBuffer += ((a.getProperty("id").asInstanceOf[Long], b.getProperty("id").asInstanceOf[Long], c.getProperty("id").asInstanceOf[Long]))
                  }
                }
              }
            }
            finalsBuffer.toList
          })

          res.length
        })
        p.println(f"Neo4J\t$startId%d,$endId%d\t${res.toString}%s\t${time.time}%d\t${time.subs("graph").time}%d\t${time.subs("relational").time}%d")
        //				Timer.printResults()
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }

    // TODO def postgresql...

    val tx = graphDb.beginTx()
    var routes = List[(Long, Long)]()
    try {
      routes = AStar.findRoutes(20, 30)
        .take(routesNum)
        .toList
    } finally {
      tx.close()
    }

    p.println("Engine\troute\tlength\ttot time\tgraph time\trelational time")
    for (_ <- 1 to iterations) {
      routes.foreach {
        case (startId, endId) =>
          System.gc()
          neo(startId, endId)
          System.gc()
          grapht(startId, endId)
      }
    }
  }

  def hybridTest(p: PrintWriter, /*boundsNum:Int,*/ iterations:Int)(implicit connection: Connection): Unit = {

    def grapht(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {
      val adaptor = new GraphtAdaptor(new Graph(new LookaheadMultiPrefetcher(40)))
      val astar = new AStarCalculator(adaptor)

      Timer.clearTimes()
      val (time, res) = Timer.timeWithResult(s"Grapht", {

        val points = Timer.time("relational", {
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
              	 						| 	AND a.id < b.id AND b.id < c.id""".stripMargin)
            .on("minLat" -> minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong)()
            .map(row => {
              val r = row.asList
              (r(0).asInstanceOf[Long], r(1).asInstanceOf[Long], r(2).asInstanceOf[Long])
            })
            .head
        })

        Timer.time("graph", astar.find(points._1, points._2))

      })
      //				p.printf("Grapht\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.dist.toLong, time.time)
      Timer.printResults()
      Timer.clearTimes()
    }
    def neo2(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {
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
                val key = n.getProperty("payload").asInstanceOf[String].substring(0, 2)
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
        //				p.printf("Neo4J\t%d,%d,%d,%d\t%d\t%d\n", minLat, minLong, maxLat, maxLong, res.dist.toLong, time.time)
        Timer.printResults()
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }
    def neo(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {
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
                ) {
                  finalsBuffer += ((a.getId(), b.getId(), c.getId()))
                }
              }
            }
          }


          val finals = finalsBuffer.toList
          astar.find(finals.head._1, finals.head._2)
        })
        p.println(f"Neo4J1\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res.dist.toLong}%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }

    // TODO: use bounds Num
    val bounds = List(
      ((-74450000, -73500010), (40301000, 40301500))
    )

    p.println("Engine\tbounds\tresults\ttime")
    for (_ <- 1 to iterations) {
      bounds.foreach {
        case ((minLat, maxLat), (minLng, maxLng)) =>
          System.gc()
          neo2(minLat, maxLat, minLng, maxLng)
          System.gc()
          grapht(minLat, maxLat, minLng, maxLng)
      }
    }
  }

  def colinear(p: PrintWriter, /*boundsNum:Int,*/ iterations:Int)(implicit connection: Connection): Unit = {

    def psql(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {

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
            .on("minLat" -> minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong)()
            //						.map( r => {
            //							val r1 = r[List[Int]]("matches")
            //							r1
            //						})
            .length
        })
        p.println(f"Grapht\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res}%d\t${time.time}%d")
        p.println(f"PostgreSQL\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res}%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }
    def neo2(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {
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
                ) {
                  count += 1
                }
              }
            }
          }

          count
        })
        p.println(f"Neo4J\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res}%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }

    val bounds = List(
      ((-74450000, -73500010), (40301000, 40301500))
    )

    p.println("Engine\tbounds\tresults\ttime")
    for (_ <- 1 to iterations) {
      bounds.foreach {
        case ((minLat, maxLat), (minLng, maxLng)) =>
          System.gc()
          neo2(minLat, maxLat, minLng, maxLng)
          System.gc()
          psql(minLat, maxLat, minLng, maxLng)
      }
    }
  }

  def threeSumTest(p: PrintWriter, iterations:Int)(implicit connection: Connection): Unit = {

    def psql(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long, target: Long)(implicit db: GraphDatabaseService): Unit = {

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
            .on("minLat" -> minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong, "target" -> target)()
            //						.map( r => {
            //							val r1 = r[List[Int]]("matches")
            //							r1
            //						})
            .length
        })
        p.println(f"Grapht\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t$res%d\t${time.time}%d")
        p.println(f"PostgreSQL\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t$res%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }
    def neo2(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long, target: Long)(implicit db: GraphDatabaseService): Unit = {
      val tx = db.beginTx()
      try {
        val (time, res) = Timer.timeWithResult(s"Neo4J", {

          val label: Label = Label.label("midset")

          // part one - identify geographically local points
          val edgeList = ListBuffer[Relationship]()
          db.getAllNodes.asScala.foreach(n => {
            if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < minLong
              && minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {

              n.getRelationships(ROAD, Direction.OUTGOING).asScala.foreach(r => {
                edgeList += r
              })
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
        p.println(f"Neo4J\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t$res%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }

    val bounds = List(
      ((-74450000, -73500010), (40301000, 40301500))
    )

    p.println("Engine\tbounds\tresults\ttime")
    for (_ <- 1 to iterations) {
      bounds.foreach {
        case ((minLat, maxLat), (minLng, maxLng)) =>
          System.gc()
          neo2(minLat, maxLat, minLng, maxLng, 5534)
          System.gc()
          psql(minLat, maxLat, minLng, maxLng, 5534)
      }
    }
  }

  def relaLevenshtein(p: PrintWriter, numQueries:Int, iterations:Int)(implicit connection: Connection): Unit = {

    def psql(query : String)(implicit db: GraphDatabaseService): Unit = {

      val tx = db.beginTx()
      try {
        val (time, res) = Timer.timeWithResult(s"Grapht", {
          SQL("SELECT id, levenshtein({query}, payload) relevance FROM edges ORDER BY relevance, id LIMIT 1")
            .on("query" -> query)
            .as(SqlParser.long("id").single)
        })
        p.println(f"Grapht\t$query%s\t$res%d\t${time.time}%d")
        p.println(f"PostgreSQL\t$query%s\t$res%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }
    def neo(query:String)(implicit db: GraphDatabaseService): Unit = {
      val tx = db.beginTx()
      try {
        val (time, res) = Timer.timeWithResult(s"Neo4J", {

          var (min, edge):(Int, Relationship) = (Int.MaxValue, null)
          db.getAllRelationships.asScala.foreach(n => {
            val cost = levenshtein(n.getProperty("payload").asInstanceOf[String], query)
            if (cost < min) {
              min = cost
              edge = n
            } else if (cost == min && n.getId() < edge.getId()) {
              edge = n
            }
          })
          edge.getId()
        })
        p.println(f"Neo4J\t$query%s\t$res%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }

    p.println("Engine\tQuery\tResults\tTime")
    val queries = (1 to numQueries).map(_ => Math.random().toString).map(md5)
    for (i <- 1 to iterations) {
      println(s"Levenshtein iteration $i")
        for (query <- queries) {
          println(s"    query $query")
          printf("        Doing neo... ")
          System.gc()
          neo(query)
          p.flush()
          printf("Done!\n")

          printf("        Doing Grapht... ")
          System.gc()
          psql(query)
          p.flush()
          printf("Done!\n")
      }
    }
  }

  def relaSubstring(p: PrintWriter, iterations:Int)(implicit connection: Connection): Unit = {

    def psql(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {

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
            .on("minLat" -> minLat, "minLong" -> minLong, "maxLat" -> maxLat, "maxLong" -> maxLong)()
            .map(r => {
              val r1 = r[List[Int]]("matches")
              r1
            })
        })
        p.println(f"Grapht\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res.length}%d\t${time.time}%d")
        p.println(f"PostgreSQL\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res.length}%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }
    def neo2(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {
      val tx = db.beginTx()
      try {
        val (time, res) = Timer.timeWithResult(s"Neo4J", {

          val label: Label = Label.label("midset")

          // part one - identify geographically local points
          var group = Map[String, ListBuffer[Long]]()
          db.getAllNodes.asScala.foreach(n => {
            if (minLat < n.getProperty("lat").asInstanceOf[Long] && n.getProperty("lat").asInstanceOf[Long] < minLong
              && minLong < n.getProperty("long").asInstanceOf[Long] && n.getProperty("long").asInstanceOf[Long] < maxLong) {
              val key = n.getProperty("payload").asInstanceOf[String].substring(0, 2)
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
        p.println(f"Neo4J\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res.length}%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }
    def neo(minLat: Long, maxLat: Long, minLong: Long, maxLong: Long)(implicit db: GraphDatabaseService): Unit = {
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
          db.findNodes(label).asScala.foreach(n => {
            val key = n.getProperty("payload").asInstanceOf[String].substring(0, 2)
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
        p.println(f"Neo4J\t$minLat%d,$minLong%d,$maxLat%d,$maxLong%d\t${res.length}%d\t${time.time}%d")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }

    val bounds = List(
      ((-74450000, -73500010), (40301000, 40301500))
    )

    p.println("Engine\tbounds\tresults\ttime")
    for (_ <- 1 to iterations) {
      bounds.foreach {
        case ((minLat, maxLat), (minLng, maxLng)) =>
          System.gc()
          neo2(minLat, maxLat, minLng, maxLng)
          System.gc()
          neo(minLat, maxLat, minLng, maxLng)
          System.gc()
          psql(minLat, maxLat, minLng, maxLng)
      }
    }
  }

  def graphTest(p: PrintWriter, routesNum:Int, iterations:Int)(implicit connection: Connection): Unit = {

    def neo2(start: Long, end: Long)(implicit db: GraphDatabaseService): Unit = {
      val tx = db.beginTx()
      try {
        val adaptor = new NeoAdaptor()(db)
        val astar = new AStarCalculator(adaptor)
        val (time, res) = Timer.timeWithResult(s"Grapht,$start,$end", {
          astar.find(start, end)
        })
        p.println(f"neo2	$end%d	${res.dist.toLong}%d	${res.length}%d	${time.time/1.0e9}%.3f")
        Timer.clearTimes()
      } finally {
        tx.close()
      }
    }
    def grapht(start: Long, end: Long): Unit = {
      val adaptor = new GraphtAdaptor(new Graph(new LookaheadMultiPrefetcher(50)))
      val astar = new AStarCalculator(adaptor)
      val (time, res) = Timer.timeWithResult(s"Grapht,$start,$end", {
        astar.find(start, end)
      })
      p.println(f"Grapht	$end%d	${res.dist.toLong}%d	${res.length}%d	${time.time/1.0e9}%.3f")
      Timer.clearTimes()
    }
    def psql3(start: Long, end: Long): Unit = {
      val adaptor = new PSQLAdaptor("points", "id", "edges", "id1", "id2")
      val astar = new AStarCalculator(adaptor)
      val (time, res) = Timer.timeWithResult(s"PSQL raw,$start,$end", {
        astar.find(start, end)
      })
      p.println(f"PSQL2	$end%d	${res.dist.toLong}%d	${res.length}%d	${time.time/1.0e9}%.3f")
      Timer.clearTimes()
    }
    def psql2(start: Long, end: Long): Unit = {
      val adaptor = new GraphtAdaptor(new Graph(new NullPrefetcher()))
      val astar = new AStarCalculator(adaptor)
      val (time, res) = Timer.timeWithResult(s"Grapht,$start,$end", {
        astar.find(start, end)
      })
      p.println(f"PSQL	$end%d	${res.dist.toLong}%d	${res.length}%d	${time.time/1.0e9}%.3f")
      Timer.clearTimes()
    }
    def psql(from: Long, to: Long)(implicit connection: Connection): Unit = {
      val (time, rs) = Timer.timeWithResult(s"Grapht,$from,$to", {
        SQL(
          "SELECT distance::bigint FROM (SELECT (astarroute({from}, {to})).*) t;")
          .on("from" -> from, "to" -> to)()
          .map((row) => {
            row[Long]("distance")
          }).head
      })
      p.println(f"PostgreSQL	$to%d	$rs%d ${time.time/1.0e9}%.3f")
    }

    val tx = graphDb.beginTx()
    var routes = List[(Long, Long)]()
    try {
      routes = AStar.findRoutes(50,60)
        .take(routesNum)
        .toList
    } finally {
      tx.close()
    }

    p.println("Engine\tEndpoint\tDistance\tHops\tTime")
    for (i <- 1 to iterations) {
      println(s"Graph iteration $i")
      routes.foreach {
        case (start, end) =>
          println(s"   Route $start->$end")

          printf("        Doing Neo4J... ")
          System.gc()
          System.gc()
          neo2(start, end)
          p.flush()
          printf("Done!\n")

          printf("        Doing Grapht... ")
          System.gc()
          grapht(start, end)
          p.flush()
          printf("Done!\n")

          printf("        Doing PostgreSQL... ")
          System.gc()
          psql3(start, end)
          printf("Done!\n")
        }
    }

  }

}
