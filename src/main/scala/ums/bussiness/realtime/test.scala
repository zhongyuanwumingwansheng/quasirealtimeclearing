package ums.bussiness.realtime
import org.json4s.JsonAST.JObject
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization
import org.json4s.native.JsonMethods._
import ums.bussiness.realtime.ApplyULinkIncreRuleToSparkStream.logError
import ums.bussiness.realtime.model.flow.UlinkIncre
/**
  * Created by zhaikaixuan on 24/08/2017.
  */
object test {
  def main(args: Array[String]): Unit = {
    implicit val format = Serialization.formats(ShortTypeHints(List()))

    try {
      val parseString =
        """{"PLT_SSN":"003341617550","TRANS_ST":"5","TRANS_ST_RSVL":"0","PROD_STYLE":"51A2",
          |"ROUT_INST_ID_CD":"","MCHNT_ID_PAY":"","TERM_ID_PAY":"","TRANS_CD_PAY":"","PAY_ST":"",
          |"TRANS_AMT":"","RSVD1":"","RSVD6":"1001"}""".stripMargin
      val JObject(message) = parse(parseString).asInstanceOf[JObject]
      val ulinkIncre = new UlinkIncre()
      println(message)
      message.map(keyValue =>
        keyValue._1 match {
          case "PLT_SSN" => ulinkIncre.setPltSsn(keyValue._2.extract[String])
          case "TRANS_ST" => ulinkIncre.setTransSt(keyValue._2.extract[String])
          case "TRANS_ST_RSVL" => ulinkIncre.setTransStRsvl(keyValue._2.extract[String])
          case "PROD_STYLE" => ulinkIncre.setProdStyle(keyValue._2.extract[String])
          case "ROUT_INST_ID_CD" => ulinkIncre.setRoutInstIdCd(keyValue._2.extract[String])
          case "MCHNT_ID_PAY" => ulinkIncre.setMchntIdPay(keyValue._2.extract[String])
          case "TERM_ID_PAY" => ulinkIncre.setTermIdPay(keyValue._2.extract[String])
          case "TRANS_CD_PAY" => ulinkIncre.setTransCdPay(keyValue._2.extract[String])
          case "PAY_ST" => ulinkIncre.setPaySt(keyValue._2.extract[String])
          case "TRANS_AMT" => ulinkIncre.setTransAmt(keyValue._2.extract[String].toDouble)
          case "RSVD1" => ulinkIncre.setdRsvd1(keyValue._2.extract[String])
          case "RSVD6" => ulinkIncre.setdRsvd6(keyValue._2.extract[String].toInt)
          case _ =>
        }
      )
    }
    catch {
      case e: Exception => println("Parse Json ERROR: ")
    }
  }
}
