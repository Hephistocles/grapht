package re.toph.hybrid_db

import java.util.HashMap
/**
  * Created by christoph on 29/04/16.
  */
package object accumulator {

  trait TAccumulator {
    def acc:(String, HashMap[String, String]) => String
  }

  class ConcatAccumulator(prop:String, sep:String) extends TAccumulator {
    override def acc: (String, HashMap[String, String]) => String = {
      (a, n) => a + sep + n.get(prop)
    }
  }

//  class CountAccumulator() extends TAccumulator {
//    override def acc: (Object,HashMap[String, Object]) => Object = (a, n) => {
//      new Integer(a.asInstanceOf[Integer].intValue() + 1)
//    }
//  }
//
//  class LastAccumulator(prop:String) extends TAccumulator {
//    override def acc: (Object,HashMap[String, Object]) => Object =
//      (_,n) => n.get(prop)
//  }
//
//  class ConstAccumulator extends TAccumulator {
//    override def acc: (Object, HashMap[String, Object]) => Object =
//      (a,_) => a
//  }
}
