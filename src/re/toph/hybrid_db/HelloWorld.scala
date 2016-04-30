package re.toph.hybrid_db

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer


/**
  * Created by christoph on 28/04/16.
  */
object HelloWorld {

  def main(args: Array[String]): Unit = {

    Timer.time("Really Null", {
      getHopDists(5, 20, NullPrefetcher)
    })
    Timer.time("Lookahead Join (1)", {
      getHopDists(5, 20, new LookaheadJoinPrefetcher(1))
    })
    var res = List[Map[String, Any]]()
    for (i <- 0 to 5) {
      res = Timer.time("Lookahead Multi (" + i + ")", {
        getHopDists(3, 1, new LookaheadMultiPrefetcher(i))
      })
    }
    res.foreach(println)
  }

  def acc(a: TAccumulator, p: java.util.HashMap[String, Object]): TAccumulator = {
    a.acc(p)
  }

  /*
    SELECT start, end, length, (ACC VERTICES CONCAT id " -> ") path, (ACC EDGES SUM dist) cost FROM
    PATHS OVER myGraph
    WHERE start = ?
    AND length <= ?);
  */
  def getHopDists(n: Int, id: Long, p: Prefetcher): List[Map[String, Any]] = {
    val g = new Graph(p)

    val results = new Result(
      edgeProps = HashMap(
        "length" -> new CountAccumulator(0),
        "path" -> new ConcatAccumulator("id2", "->", "(" + id + ")")
      ),
      vertexProps = HashMap(
        "start" -> new ConstAccumulator(id),
        "end" -> new LastAccumulator("id", id),
        "dist" -> new SumAccumulator("dist", 0L)
      ))

    val condition =
      new AndCondition(
        new EqualityCondition[Long]("start", id, Constant()),
        new LessThanOrEqualCondition[Int]("length", n, Increasing(Some(1)))
      )

    query(g, id, results, condition).toList
  }

  def query(g: Graph, id: Long, sofar: Result, condition: CompletionCondition): ListBuffer[Map[String, Any]] = {

    val vertex = g.getVertex(id)

    // calculate a new intermediate result, taking into account this vertex's properties
    val vertexResult = sofar.mapVertex((acc) => acc.acc(vertex.properties))

    val pathResults = ListBuffer[Map[String, Any]]()

    if (condition.check(vertexResult)) {
      pathResults += vertexResult.result
    }

    // if there's no point in constructing longer paths - stop!
    if (!condition.nextSatisfiable(vertexResult)) {
      return pathResults
    }

    // Otherwise let's look at neighbours
    vertex.edges.foreach(e => {
      val edgeResult = vertexResult.mapEdges(a => a.acc(e.properties))

      // explore neighbours to find more path results and merge the results in!
      // TODO: generalise this with a priority queue or something
      //       also important to avoid stackoverflows on large expansions
      pathResults ++= query(g, e.to, edgeResult, condition)
    })

    pathResults
  }

}