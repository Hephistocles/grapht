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

  def timeWithResult[R](name:String, block: => R): (TimerResult, R) = {

    val last = root
    root = last.addSub(name)

    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()

    val diff = t1 - t0

    root.addReading(diff)
    val th = root
    root = last
    (th, result)

  }

  def time[R](name:String, block: => R): R = {
    timeWithResult(name, block)._2
  }

  def timeString(time: Long) : String = {

    if (time < 10000000) {
      val newTime = time/1000000.0
      f"$newTime%6.2fms"
    } else {
      val newTime = time/1000000
      f"$newTime%6dms"
    }
  }

  def printTree(timerResult: TimerResult, maxDepth:Int, depth:Int = 0, parentCount:Int=1):Unit = {

    println(f"${" "*(depth*2) + timerResult.name}%-30s"
      + s"${timeString(timerResult.time)} tot ${timeString(timerResult.time/timerResult.count)} avg    "
      + f"${timerResult.count}%6d tot ${timerResult.count/parentCount}%6d avg")

    if (depth>=maxDepth) return

    timerResult.subs.toList
      .sortBy({
        case (_, tr) => tr.time/tr.count
      })
      .foreach({
        case (_, tr) => printTree(tr, maxDepth, depth+1, timerResult.count * parentCount)
      })


  }

  def printResults(depth:Int=5): Unit = {

    val t1 = System.nanoTime()
    val diff = (t1 - initTime)/1000000
    root.addReading(diff)

    printTree(root, maxDepth=depth)
  }
}
