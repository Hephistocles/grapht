package re.toph.hybrid_db

import scala.collection.mutable

class TimerResult(_name:String) {
  var time=0L
  var subs=mutable.Map[String, TimerResult]()
  var count=0
  var name=_name
  def addReading(newTime:Long): Unit = {
    time += newTime
    count += 1
  }
  def addSub(n:String): TimerResult = {
    subs.get(n) match {
      case Some(x) => x
      case None =>
        val x = new TimerResult(n)
        subs += (n -> x)
        x
    }
  }
}

/**
  * Created by christoph on 28/04/16.
  */
object Timer {

  var root = new TimerResult("Root")
  var initTime = System.nanoTime()

  def clearTimes() :Unit = {

    root = new TimerResult("Root")
    initTime = System.nanoTime()

  }


  def time[R](name:String, block: => R): R = {

    val last = root
    root = last.addSub(name)

    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()

    val diff = (t1 - t0)/1000000

    root.addReading(diff)
    root = last
    result
  }

  def printTree(timerResult: TimerResult, maxDepth:Int, depth:Int = 0):Unit = {

    printf("%-30s %6d tot %6d avg %6d\n", " "*(depth*2) + timerResult.name, timerResult.time, timerResult.time/timerResult.count, timerResult.count)
    if (depth>=maxDepth) return

    timerResult.subs.toList
      .sortBy({
        case (_, tr) => tr.time
      })
      .foreach({
        case (_, tr) => printTree(tr, maxDepth, depth+1)
      })


  }

  def printResults(depth:Int=5): Unit = {

    val t1 = System.nanoTime()
    val diff = (t1 - initTime)/1000000
    root.addReading(diff)

    printTree(root, maxDepth=depth)
  }
}
