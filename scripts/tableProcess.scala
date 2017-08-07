//输入为以txt文本格式的BMS_STL_INFO，SYS_GROUP_ITEM_INFO，SYS_TXN_CODE_INFO，SYS_MAP_ITEM_INFO，每一列以"|"分隔；
//Ulink增值和传统同样以txt文本格式，每一列以空格分隔；
val BMS_STL_INFO_loc = ""
val SYS_GROUP_ITEM_INFO_loc = ""
val SYS_TXN_CODE_INFO_loc = ""
val SYS_MAP_ITEM_INFO_loc = ""
case class BMS_STL_INFO_Class()
case class SYS_GROUP_ITEM_INFO_Class()
case class SYS_TXN_CODE_INFO_Class()
case class SYS_MAP_ITEM_INFO()

sc.textFile(BMS_STL_INFO_loc).split("|").map{
  row => BMS_STL_INFO

}
