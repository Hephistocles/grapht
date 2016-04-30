package re.toph.hybrid_db

/**
  * Created by christoph on 30/04/16.
  */

abstract class VarDirection[T]
case class Increasing[T](by:Option[T]) extends VarDirection[T]
case class Decreasing[T](by:Option[T]) extends VarDirection[T]
case class Constant[T]() extends VarDirection[T]
case class Variable[T]() extends VarDirection[T]

trait CompletionCondition {
  def check(r:Result):Boolean
  def nextSatisfiable(r:Result):Boolean
}

case class OrCondition(cs:CompletionCondition*) extends CompletionCondition {
  override def check(r:Result) = ! cs.forall(c => !c.check(r))
  override def nextSatisfiable(r: Result): Boolean = ! cs.forall(c => !c.nextSatisfiable(r))
}
case class AndCondition(cs:CompletionCondition*) extends CompletionCondition {
  override def check(r:Result) = cs.forall(c =>c.check(r))
  override def nextSatisfiable(r: Result): Boolean = cs.forall(c => c.nextSatisfiable(r))
}

// NB we only have comparisons to const here - might want between two rows too!
case class GreaterThanCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
  override def check(r: Result) = num.gt(r.get(prop).asInstanceOf[T], item)
  override def nextSatisfiable(r: Result): Boolean = d match {
    // first two cases are easy - we know how much is de/increasing by so can just check
    case Increasing(Some(x)) =>
      num.gt(num.plus(r.get(prop).asInstanceOf[T], x), item)
    case Decreasing(Some(x)) =>
      num.gt(num.minus(r.get(prop).asInstanceOf[T], x), item)
    case Constant() => check(r)
    case _ => // lacking any further information, assume it might be satisfied next time
      true
  }
}

case class GreaterThanOrEqualCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
  override def check(r: Result) = num.gteq(r.get(prop).asInstanceOf[T], item)
  override def nextSatisfiable(r: Result): Boolean = d match {
    // first two cases are easy - we know how much is de/increasing by so can just check
    case Increasing(Some(x)) =>
      num.gteq(num.plus(r.get(prop).asInstanceOf[T], x), item)
    case Decreasing(Some(x)) =>
      num.gteq(num.minus(r.get(prop).asInstanceOf[T], x), item)
    // if I'm equal and I decrease at all, next turn is invalid (so check I am currently precisely greater)
    case Decreasing(None) =>
      num.gt(r.get(prop).asInstanceOf[T], item)
    case Constant() => check(r)
    case _ => // lacking any further information, assume it might be satisfied next time
      true
  }
}

case class LessThanCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
  override def check(r: Result) = num.lt(r.get(prop).asInstanceOf[T], item)
  override def nextSatisfiable(r: Result): Boolean = d match {
    // first two cases are easy - we know how much is de/increasing by so can just check
    case Increasing(Some(x)) =>
      num.lt(num.plus(r.get(prop).asInstanceOf[T], x), item)
    case Decreasing(Some(x)) =>
      num.lt(num.minus(r.get(prop).asInstanceOf[T], x), item)
    case Constant() => check(r)
    case _ => // lacking any further information, assume it might be satisfied next time
      true
  }
}

case class LessThanOrEqualCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
  override def check(r: Result) = num.lteq(r.get(prop).asInstanceOf[T], item)
  override def nextSatisfiable(r: Result): Boolean = d match {
    // first two cases are easy - we know how much is de/increasing by so can just check
    case Increasing(Some(x)) =>
      num.lteq(num.plus(r.get(prop).asInstanceOf[T], x), item)
    case Decreasing(Some(x)) =>
      num.lteq(num.minus(r.get(prop).asInstanceOf[T], x), item)
    // if I'm equal and I increase at all, next turn is invalid (so check I am currently precisely lesser)
    case Increasing(None) =>
      num.lt(r.get(prop).asInstanceOf[T], item)
    case Constant() => check(r)
    case _ => // lacking any further information, assume it might be satisfied next time
      true
  }
}
case class EqualityCondition[T](prop:String, item:T, d:VarDirection[T])(implicit num:Numeric[T]) extends CompletionCondition {
  override def check(r: Result) = {
    val v = r.get(prop)
    num.equiv(v.asInstanceOf[T], item)
  }
  override def nextSatisfiable(r:Result) : Boolean = d match {
    // if we're moving, next turn can only be possible if we're not currently equal
    case Increasing(_) | Decreasing(_) =>
      ! num.eq(r.get(prop).asInstanceOf[T], item)
    case Constant() => check(r)
    case _ => true
  }
}
