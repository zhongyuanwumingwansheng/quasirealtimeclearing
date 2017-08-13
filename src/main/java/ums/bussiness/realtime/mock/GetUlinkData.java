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
import org.apache.log4j.Logger;

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

    public void produceEachTopic(String topic, String fileNames){
        //从文件读取，假设一行是一笔交易，这种假设也与需求相符
        //TODO, 包含海量数据的大文件的处理
        if(fileNames.equals("")||fileNames.matches("\\s+")){
            System.out.println("Files not exists:"+fileNames);
            return;
        }
        FileInputStream file = null;
        BufferedReader reader = null;
        InputStreamReader inputFileReader = null;
        String tempString=null;
        String[]fileNameAry=fileNames.split(",");
        for(int i=0;i<fileNameAry.length;i++){
            String fileName=fileNameAry[i];
            try {
                file = new FileInputStream(fileName);
                inputFileReader = new InputStreamReader(file, setting.getString("coding"));
                reader = new BufferedReader(inputFileReader, setting.getInt("inputBuffer"));
                // 一次读入一行，直到读入null为文件结束
                while ((tempString = reader.readLine()) != null) {
                    StringBuilder result=new StringBuilder();
                    result.append("{");
                    long timestamp = System.currentTimeMillis();
                    String key = topic+"_"+String.valueOf(timestamp);
                    //TODO.the data has problem. each line's length of data is not exactly the same
                    if(topic.equals("ULinkIncre")){
                        //TODO,Hard Code.
                        //for test chinese
                        //System.out.println(getGbKSubStr(tempString, 41,81));
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",8))+"\":\""+getGbKSubStr(tempString, 212,213)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",10))+"\":\""+getGbKSubStr(tempString, 216,217)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",29))+"\":\""+getGbKSubStr(tempString, 381,385)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",99))+"\":\""+getGbKSubStr(tempString, 1307,1332)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",104))+"\":\""+getGbKSubStr(tempString, 1381,1396)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",105))+"\":\""+getGbKSubStr(tempString, 1397,1415)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",109))+"\":\""+getGbKSubStr(tempString, 1447,1455)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",112))+"\":\""+getGbKSubStr(tempString, 1499,1500)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",124))+"\":\""+getGbKSubStr(tempString, 1657,1669)+"\",");
                        result.append("\""+setting.getString(String.format("ULinkIncre.p%d",160))+"\":\""+getGbKSubStr(tempString, 2367,2379)+"\"}");
                    }
                    if(topic.equals("ULinkNormal")){
                        String[]values=tempString.split("\\|");
                        //TODO,Hard Code
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",4))+"\":\""+values[4].trim()+"\",");
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",5))+"\":\""+values[5].trim()+"\",");
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",7))+"\":\""+values[7].trim()+"\",");
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",25))+"\":\""+values[25].trim()+"\",");
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",33))+"\":\""+values[33].trim()+"\",");
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",34))+"\":\""+values[34].trim()+"\",");
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",42))+"\":\""+values[42].trim()+"\",");
                        result.append("\""+setting.getString(String.format("ULinkNormal.p%d",61))+"\":\""+values[61].trim()+"\"}");
                    }
                    //按需求逐笔读取
                    //producer.send(new KeyedMessage<String, String>(topic, key, new String(tempString.getBytes(), "gbk")));
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



    }

    public static void main(String args[]){
        long beginTime=System.currentTimeMillis();
        GetUlinkData kafkaProducer = new GetUlinkData();
        kafkaProducer.produce();
        long endTime=System.currentTimeMillis();
        System.out.println("Sending data costs:" + String.valueOf(endTime-beginTime) + "Millis");
    }

    /**
     *
     * @param orignal the value for substr
     * @param beginByte the beginning byte index, inclusive.
     * @param endByte the ending byte index, exclusive.
     * @return
     */
    public String getGbKSubStr(String orignal, int beginByte, int endByte){
        String result=null;
        int length=endByte-beginByte;
        byte[]bytes=new byte[length];
        try{
            byte[]originBytes=orignal.getBytes("gbk");
            for (int i = beginByte; i < endByte; i++){
                bytes[i-beginByte]=originBytes[i];
            }
            result = new String(bytes, "gbk").trim();
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            return result;
        }
    }

}