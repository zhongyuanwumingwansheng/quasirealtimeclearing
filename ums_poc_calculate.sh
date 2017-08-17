#!/bin/bash
#使用示例：
#交易监控数据更新服务:trans_monitor_calculate_bin.sh start|stop|restart bwg
#网络发卡衍生字段计算:trans_monitor_calculate_bin.sh start|stop|restart netcard
#非金数据更新启动脚本:trans_monitor_calculate_bin.sh start|stop|restart ntfm
#官网日志:trans_monitor_calculate_bin.sh start|stop|restart official
#全部启停:trans_monitor_calculate_bin.sh start|stop|restart all

#####变量校验提示####

########变量区########
#-----任务运行----#
#注意:
#yarn-cluster:该模式运行的任务Driver集群中启动，本地PID  kill掉，会在其他机器启动Driver,导致Stop不能停止该模式下Job
#yarn-client:客户端模式，Driver在客户端启动，本地PID kill,Driver不会再其他机器启动，Stop停止有效。
master="yarn-client"
#---kafka zookeeper host---#
kafkaZkHost=":q
：:2181"
#---kafka broker list---#
broker_list="172.17.1.146:9092"
#---hbase---#
hbaseZkHost="182.180.23.85:2181,182.180.23.88:2181,182.180.23.90:2181"
#---redis---#
redisZkHost="182.180.23.69:2181,182.180.23.70:2181,182.180.23.71:2181"
#---redis zk 代理地址
codisZkdir="/zk/codis/db_trans_monitor/proxy"

#---kafka-Group---#
tfm_group="tfm-data-group"
netcard_group="netcard-log-group"
nf_group="nf-data-group"
official_group="official-log-group"
#---kafka-Topic---#
tfm_topic="tp-bgw"
netcard_topic="topic_onfm_from_ola"
nf_topic="tp-ntfm"
official_topic="topic_onfm_from_sac"
derive_result_topic="tp-trans-log"
#----日志等级----#
loglevel="ERROR"
#----服务状态监控轮询时间间隔(单位秒) 建议>30秒
pollInterval=15
#-----服务最大重启次数（自动重启）----#
restartCount=0
maxRestartCount=5
#----Mysql（记录服务状态）-----#
hostname=182.180.23.65
dbname=onfprm
username=onfprm
password=onfprm12345
tableName=realtime_job
######################



########代码区#######
#Lib依赖包拼接，查找脚本路径下Lib文件#
shell=$0
current=`cd $(dirname $0);pwd`
cd $current
printf $current
cd "$current/target"
lib_files=`ls lib/`
jars=""
for lib in $lib_files
do
    jars="$current/target/lib/$lib",$jars
done
printf $jars

#Shift:参数左移，同等于NAME=$2 #
startstop=$1
shift
NAME=$1
shift

#log4j配置#
driver_log4j_config="spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${current}/config/log4j.properties"
executor_log4j_config="spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${current}/config/log4j.properties"

#启动作业#
start(){
#提交作业
case $NAME in
incremental)
        printf "incremental"
      #提交服务作业#
        spark-submit --class ums.bussiness.realtime.ApplyULinkIncreRuleToSparkStream --supervise --master $master --driver-class-path $current/target/config/ --jars $jars $current/target/realtimeClassify-1.0-SNAPSHOT.jar $loglevel #>> $OUTFILE 2>&1 &
        #echo $! > $PIDFILE
	;;
normal)
      #提交服务作业#
        nohup spark-submit --class ums.bussiness.realtime.ApplyULinkNormalRuleToSparkStream2 --supervise --master $master --driver-class-path $current/target/config/ --jars $jars $current/target/realtimeClassify-1.0-SNAPSHOT.jar $loglevel #>> $OUTFILE 2>&1 &
        #echo $! > $PIDFILE
        ;;
*)
        echo "--找不到服务"
esac
}

case $startstop in
start)
  start
;;
*)
echo "使用: $NAME.sh {start|stop|restart} incremental|normal"
esac
