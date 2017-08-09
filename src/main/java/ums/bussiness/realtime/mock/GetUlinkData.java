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
            inputFileReader = new InputStreamReader(file, "utf-8");
            reader = new BufferedReader(inputFileReader);
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                StringBuilder result=new StringBuilder();
                result.append("{");
                long timestamp = System.currentTimeMillis();
                String key = topic+"_"+String.valueOf(timestamp);
                if(topic.equals("ULinkIncre")){
                    String[]values=tempString.replaceAll("  ","!!!!").split("\\s+");
                    System.out.println(values.length);
                    //TODO,Hard Code.and the data has problem
                    result.append(setting.getString(String.format("ULinkIncre.p%d",8))+":"+values[8]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",29))+":"+values[29]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",99))+":"+values[99]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",104))+":"+values[104]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",105))+":"+values[105]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",109))+":"+values[109]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",111))+":"+values[111]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",112))+":"+values[112]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",124))+":"+values[124]+",");
                    result.append(setting.getString(String.format("ULinkIncre.p%d",160))+":"+values[160]+"}");
                }
                if(topic.equals("ULinkNormal")){
                    String[]values=tempString.split("\\|");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",4))+":"+values[4]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",5))+":"+values[5]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",7))+":"+values[7]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",25))+":"+values[25]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",33))+":"+values[33]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",34))+":"+values[34]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",42))+":"+values[42]+",");
                    result.append(setting.getString(String.format("ULinkNormal.p%d",61))+":"+values[61]+"}");
                }
                //System.out.println(result);
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
