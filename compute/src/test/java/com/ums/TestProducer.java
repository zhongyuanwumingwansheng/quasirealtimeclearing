package com.ums;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Properties;
import org.json4s.*;


/**
 * Created by zhaikaixuan on 27/07/2017.
 */
public class TestProducer {
    private static Producer<String, String> producer;
    @BeforeClass
    public static void setUp(){
        Properties props = new Properties();
        props.setProperty("metadata.broker.list","localhost:9092");
        props.setProperty("serializer.class","kafka.serializer.StringEncoder");
        props.put("request.required.acks","1");
        props.put("producer.type","async");
        ProducerConfig config = new ProducerConfig(props);
        //创建生产这对象
        producer = new Producer<String, String>(config);

    }
    @Test
    public void testProducer(){
        KeyedMessage<String, String> data1 = new KeyedMessage<String, String>("ulink_incremental","test kafka");
        KeyedMessage<String, String> data2 = new KeyedMessage<String, String>("ulink_traditional","hello world");

        try{
            while(true){
                producer.send(data1);
                producer.send(data2);
                System.out.println("hello world");
                Thread.sleep(1000);
                break;
            }

        } catch (Exception e){
            e.printStackTrace();
        }

    }
    @AfterClass
    public static void tearDown(){
        producer.close();
    }
}
