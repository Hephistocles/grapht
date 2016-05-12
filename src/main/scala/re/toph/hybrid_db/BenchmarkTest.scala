package re.toph.hybrid_db

/**
  * Created by christoph on 12/05/16.
  */
trait BenchmarkTest {
  val timeAll: (List[(String, ()=>List[Map[String,Any]])], Int) => Unit =
    (l, iterations) => {
      for (i <- 0 to iterations) {
        l.foreach({
          case (s, f) => Timer.time(s, f())
        })
      }
    }
}
