package re.toph.hybrid_db

import java.util

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
  val results = mutable.Map[String, TimerResult]()

  val stack = new util.Stack[TimerResult]()

  var root = new TimerResult("Root")
  val initTime = System.nanoTime()

//  stack.push(root)

  def time[R](name:String, block: => R, printResults: Boolean = false): R = {

//    stack.push(t)

    val last = root
    root = last.addSub(name)

    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()

    val diff = (t1 - t0)/1000000

//    val (time, count) = results.getOrElse(name, (0L,0))
//    results += (name -> (time + diff, count+1))
    root.addReading(diff)
    root = last

//    stack.pop() // this should equal t, but we just want it gone from the stack
//    if (stack.empty()) {
//      // this must have been a top-level time
//      results.get(name) match {
//        case Some(tr) => tr.addSub(t)
//        case None => results += (name->t)
//      }
//    } else {
//      stack.peek().addSub(t)
//    }

//    if (printResults)
//      println(s"Time for $name: ${diff}ms (${time + diff}ms total, ${(time+diff)/(count+1)}ms avg)")

    result
  }

  def printTree(timerResult: TimerResult, depth:Int = 0):Unit = {

    printf("%-30s %6d tot %6d avg %6d\n", " "*(depth*2) + timerResult.name, timerResult.time, timerResult.time/timerResult.count, timerResult.count)
    timerResult.subs.toList
      .sortBy({
        case (_, tr) => tr.time
      })
      .foreach({
        case (_, tr) => printTree(tr, depth+1)
      })


  }

  def printResults(): Unit = {

    val t1 = System.nanoTime()
    val diff = (t1 - initTime)/1000000
    root.addReading(diff)

    return printTree(root)

    var (maxName, (maxTime, maxCount)) = (0, (1, 1))
    val res = results.toList.sortBy({
      case (name, tr) =>
        maxName = Math.max(maxName, name.length())
        maxTime = Math.max(maxTime, tr.time.toString.length())
        maxCount = Math.max(maxCount, tr.count.toString.length())
        tr.time/tr.count
    })

    maxName = Math.max(maxName, "VARIANT NAME".length())
    maxTime = Math.max(maxTime, "TIME".length())
    maxCount = Math.max(maxCount, "COUNT".length())
    val titleformat = "+%-" + maxName + "s+%-" + maxTime + "s+%-" + maxCount + "s+\n"
    printf(titleformat, "-"*maxName, "-"*maxTime, "-"*maxCount)
    printf(titleformat, " VARIANT NAME ", " TIME ", " COUNT ")
    val format = "| %-" + maxName + "s | %" + maxTime + "d | %" + maxCount + "d |\n"
    res.foreach({
      case (name, tr) =>
        printf(format, name, tr.time, tr.count)
//        println(s"Time for $name: ${time}ms (${(time)/(count)}ms avg)")
  })
    printf(titleformat, "-"*maxName, "-"*maxTime, "-"*maxCount)
//      _._1/_._2
//      r => {
//      val (name, (time, count)) = r
//      return time/count
//    ).foreach(println)

  }
}
