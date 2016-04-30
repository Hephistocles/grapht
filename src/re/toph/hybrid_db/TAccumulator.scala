package re.toph.hybrid_db

import java.util.HashMap

/**
  * Created by christoph on 30/04/16.
  */

  trait TAccumulator {
    def acc:(HashMap[String, Object]) => TAccumulator
    def result: Any
  }

  class ConcatAccumulator(prop:String, sep:String, init:String) extends TAccumulator {
    override def acc: (HashMap[String, Object]) => TAccumulator = {
      n => new ConcatAccumulator(prop, sep, init + sep + n.get(prop).toString)
    }
    override def result: Any = init
  }

  class SumAccumulator[T](prop:String, init:T)(implicit num: Numeric[T]) extends TAccumulator {
    override def acc: (HashMap[String, Object]) => TAccumulator =
      n => new SumAccumulator(prop, num.plus(init, n.get(prop).asInstanceOf[T]))
    override def result: Any = init
  }

  class CountAccumulator(init:Int) extends TAccumulator {
    override def acc: (HashMap[String, Object]) => TAccumulator =
      n => new CountAccumulator(init + 1)
    override def result: Any = init
  }

  class LastAccumulator[T](prop:String, init:T) extends TAccumulator {
    override def acc: (HashMap[String, Object]) => TAccumulator =
      n => new LastAccumulator[T](prop, n.get(prop).asInstanceOf[T])
    override def result: Any = init
  }

  class ConstAccumulator[T](init:T) extends TAccumulator {
    override def acc: (HashMap[String, Object]) => TAccumulator =
      _ => this
    override def result: Any = init
  }
