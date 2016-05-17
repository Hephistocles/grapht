package re.toph.hybrid_db

/**
  * Created by christoph on 12/05/16.
  */
trait BenchmarkTest {
  val timeAll: (List[(String, ()=>Any)], Int) => Unit =
    (l, iterations) => {
      for (i <- 0 to iterations-1) {
        l.foreach({
          case (s, f) => {
            Timer.time(s, f())
          }
        })
      }
    }
}
