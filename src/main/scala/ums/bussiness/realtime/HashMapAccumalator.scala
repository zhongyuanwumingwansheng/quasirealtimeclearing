package ums.bussiness.realtime

import org.apache.spark.AccumulatorParam

/**
  * Created by zhaikaixuan on 03/08/2017.
  */
object HashMapAccumalatorParam extends AccumulatorParam[Map[String, Double]]{
  override def zero(initialValue: Map[String, Double]) = {
    initialValue
  }

  override def addInPlace(r1: Map[String, Double], r2: Map[String, Double]):Map[String, Double]= {
    r1 ++ r2.map{ case (k, v) => k -> (v + r1.getOrElse(k, 0.toDouble))}
  }
}
