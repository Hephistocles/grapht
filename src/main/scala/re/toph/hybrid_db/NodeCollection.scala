package re.toph.hybrid_db

import java.util
import java.util.{Comparator, PriorityQueue}

/**
  * Created by christoph on 09/05/16.
  */
trait NodeCollection[E] {
  def add(node :E) : Unit
  def get() : E
  def isEmpty() : Boolean
}

class NodeStack[E] extends NodeCollection[E] {
  val stack = new util.Stack[E]()
  override def add(node : E) = stack.push(node)
  override def get() = stack.pop()
  override def isEmpty() = stack.empty()
}

class NodeQueue[E] extends NodeCollection[E] {
  val queue = new util.LinkedList[E]()
  override def add(node : E) = queue.add(node)
  override def get() = queue.poll()
  override def isEmpty() = queue.isEmpty
}

// TODO: (maybe) rewrite this to expose a single "Add at priority X" so a higher level can use this as a stack or queue
class NodePriorityQueue[E](c:Comparator[E]) extends NodeCollection[E] {
  val queue = new PriorityQueue[E](c)
  override def add(node : E) = queue.add(node)
  override def get() = queue.poll()
  override def isEmpty() = queue.isEmpty
}
