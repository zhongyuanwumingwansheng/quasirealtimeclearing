package ums.bussiness.realtime.mock;

/**
 * Created by root on 8/3/17.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.typesafe.config.*;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetUlinkData {
    public Producer<String, String> producer;

    public Map<String, String> topics;

    public static Config setting;

    static {
        setting= ConfigFactory.load();
    }


    public GetUlinkData(){
        //两类topics,UlinkNoraml UlinkIncre
        //TODO,Hard Code
        topics = new HashMap<String, String>() {
            {
                put("ULinkNormal", setting.getString("ULinkNormal.path"));
                put("ULinkIncre", setting.getString("ULinkIncre.path"));
            }
        };
        //读取配置
        Properties props = new Properties();
        //此处配置的是kafka的端口
        //TODO,Hard Code
        props.put("metadata.broker.list", setting.getString("kafkaHost"));

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类


        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");
        //生成 kafkaProducer
        producer = new Producer<String, String>(new ProducerConfig(props));

    }

    public void produce() {
        for (Map.Entry<String, String> entry : topics.entrySet()) {
            produceEachTopic(entry.getKey(), entry.getValue());
        }
        producer.close();
    }

    public void produceEachTopic(String topic, String fileName){
        //从文件读取，假设一行是一笔交易，这种假设也与需求相符
        //TODO, 包含海量数据的大文件的处理
        FileInputStream file = null;
        BufferedReader reader = null;
        InputStreamReader inputFileReader = null;
        String tempString=null;
        try {
            file = new FileInputStream(fileName);
            //inputFileReader = new InputStreamReader(file, setting.getString("coding"));
            inputFileReader = new InputStreamReader(file);
            reader = new BufferedReader(inputFileReader);
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                StringBuilder result=new StringBuilder();
                result.append("{");
                long timestamp = System.currentTimeMillis();
                String key = topic+"_"+String.valueOf(timestamp);
                //TODO.the data has problem. each line's length of data is not exactly the same
                if(topic.equals("ULinkIncre")){
                    //TODO,Hard Code.
                    result.append(setting.getString(String.format("ULinkIncre.p%d",8))+":"+new String(tempString.substring(212,213).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",10))+":"+new String(tempString.substring(216,217).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",29))+":"+new String(tempString.substring(381,385).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",99))+":"+new String(tempString.substring(1307,1332).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",104))+":"+new String(tempString.substring(1381,1396).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",105))+":"+new String(tempString.substring(1397,1415).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",109))+":"+new String(tempString.substring(1447,1455).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",112))+":"+new String(tempString.substring(1499,1500).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",124))+":"+new String(tempString.substring(1657,1669).getBytes(), "gbk")+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",160))+":"+new String(tempString.substring(2367,2379).getBytes(), "gbk")+"}");
                }
                if(topic.equals("ULinkNormal")){
                    String[]values=tempString.split("\\|");
                    //TODO,Hard Code
                    result.append(setting.getString(String.format("ULinkNormal.p%d",4))+":"+values[4]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",5))+":"+values[5]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",7))+":"+values[7]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",25))+":"+values[25]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",33))+":"+values[33]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",34))+":"+values[34]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",42))+":"+values[42]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",61))+":"+values[61]+"}");
                }
                //按需求逐笔读取
                producer.send(new KeyedMessage<String, String>(topic, key, result.toString()));
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }finally {
            if (reader != null) {
                try{
                    reader.close();
                    file.close();
                    inputFileReader.close();
                }catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
        }


    }

    public static void main(String args[]){
        GetUlinkData kafkaProducer = new GetUlinkData();
        kafkaProducer.produce();
/*        try{
            StringBuilder result=new StringBuilder();
            result.append("{");
            String tempString="003353529294 20170722 103406 02 02S22154 ASPOS1-23                                                                                            0505072289837085411155403353529294W                              0 0 0 0 0 N 20170722103506 30 48020000    4348029999                01 603000320108                                                     08 0 05   05   05   05   5 51 A2 51A2 S22 02 A AS22 X080 600027DF9C D + N N                    0200                             0000   0002 134694059395582767                       00 BK                                  00 7  FFFFFF         99    99 190000 1550         0722103407 000863 103407 20170722 20170722 920     82    0            03353529294W             0000 11                                                             48024500    100100      1                    2989513c7bba4320921cfa3cccafae28                 a1276a9dc63044f3bf385208ba975bb1 5411 898370854111554 4610 ****************************************                                                                                            2c90ef045893ad330158a032d9ce0944                 88396449                                     156 000001       00 0                                                                                                    1  000000 48020000    0 3748020000                48024500    48024500                1000000014  898370854111554 88396449           5411 000001       000863       02S221X1 11111111111                    0 0 0 0 ULINK    0000 20170722 20170722 103407                                  4008612001201707222034700308                                      1550         1550         0            -1                                                                                                                                                                           20170722 20170722                                                                                                                        0            0                                                                                                                  1550                                              4                                                                                                                                                                                                    1001         -1           2017-07-22-10:34:06.800602 2017-07-22-10:34:08.306852 POS3                                                                                                                                                                                                                                                                                                        20170722103406003353529294";
            result.append(setting.getString(String.format("ULinkIncre.p%d",8))+":"+new String(tempString.substring(212,213).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",10))+":"+new String(tempString.substring(216,217).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",29))+":"+new String(tempString.substring(381,385).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",99))+":"+new String(tempString.substring(1307,1332).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",104))+":"+new String(tempString.substring(1381,1396).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",105))+":"+new String(tempString.substring(1397,1415).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",109))+":"+new String(tempString.substring(1447,1455).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",112))+":"+new String(tempString.substring(1499,1500).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",124))+":"+new String(tempString.substring(1657,1669).getBytes(), "gbk")+",");
            result.append(setting.getString(String.format("ULinkIncre.p%d",160))+":"+new String(tempString.substring(2367,2379).getBytes(), "gbk")+"}");
            System.out.println(tempString.indexOf("0505072289837085411155403353529294W"));
            System.out.println(("0505072289837085411155403353529294W").length());
            System.out.println(tempString.substring(381,385));
            System.out.println(result);
        }catch (Exception ex){
            ex.printStackTrace();
        }*/
/*        String t1=" 1     2";
        String t2="*1*****2";
        System.out.println(t1.replaceAll("  ","!").split("\\s+").length);
        System.out.println(t1.split(" ").length);
        System.out.println(t2.split("\\*").length);*/
/*        String test="{MSG_TYPE:0200,PROC_CODE:000000,SER_CONCODE:00,TXN_AMT:297.00,TID:20205035,MID:898510158121415,TRAN_STATUS:1,RESP_CODE:00}";
        try{
            JSONObject JItem = new JSONObject(test.toString());
            System.out.println(JItem.getString("MID"));
        }catch (Exception ex){
            ex.printStackTrace();
        }*/
    }


}
