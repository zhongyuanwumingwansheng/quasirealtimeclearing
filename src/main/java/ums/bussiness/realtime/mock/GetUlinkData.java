package ums.bussiness.realtime.mock;

/**
 * Created by root on 8/3/17.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GetUlinkData {
    public Producer<String, String> producer;

    public Map<String, String> topics;



    public GetUlinkData(){
        //两类topics,UlinkNoraml UlinkIncre
        topics = new HashMap<String, String>() {
            {
                topics.put("UlinkNormal", "UlinkNormal data file path");
                topics.put("UlinkIncre", "UlinkIncre data file path");
            }
        };
        //读取配置
        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", "192.168.193.148:9092");

        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

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
                long timestamp = System.currentTimeMillis();
                String key = topic+"_"+String.valueOf(timestamp);
                //按需求逐笔读取
                producer.send(new KeyedMessage<String, String>(topic, key, tempString));
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
            producer.close();
        }


    }
}
