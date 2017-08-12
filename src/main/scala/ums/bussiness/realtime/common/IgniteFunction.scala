package ums.bussiness.realtime.common
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import scala.util.control.Breaks._

class IgniteFunction {

  @QuerySqlFunction
  def sqr(x: Int): Int = x * x

  /**
    * 该函数的含义如下：
        IF 条件=值1 THEN
        　　　　RETURN(翻译值1)
        ELSIF 条件=值2 THEN
        　　　　RETURN(翻译值2)
        　　　　......
        ELSIF 条件=值n THEN
        　　　　RETURN(翻译值n)
        ELSE
        　　　　RETURN(缺省值)
        END IF
    * @param input
    * @return
    */
  @QuerySqlFunction
  def decode(input:String): Int = {
    //order by decode(trim(mapp_main),'1',1,2), decode(apptype_id, 1,1,86,2,74,3,18,4,39,5,40,6,68,7)
    val split_str = input.split(",")
    var output = split_str(split_str.size -1)
    val condition = split_str(0).trim
    for (i <- 1 until split_str.size - 1 if i%2!=0) {
      val parameter = split_str(i).trim
      if (condition == parameter || condition.equals(parameter)) {
        output = split_str(i + 1)
      } else {
      }
    }
    return Integer.valueOf(output)
  }

  def main(args: Array[String]): Unit = {
//    val igniteFunction = igniteFunction
    val decodeStr = "68,1,1,86,2,74,3,18,4,39,5,40,6,68,7"
    new IgniteFunction().decode(decodeStr)
  }
}
