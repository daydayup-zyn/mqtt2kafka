package com.zhbr.mqtt;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName MqttKafkaProducer
 * @Description TODO
 * @Autor yanni
 * @Date 2020/9/2 14:25
 * @Version 1.0
 **/
public class MqttKafkaProducer {

    private static Logger log = Logger.getLogger(MqttKafkaProducer.class);
    private static Producer<String, String> producer;

    public static void pushData(String msgData) {
        InputStream in = MqttKafkaProducer.class.getClassLoader().getResourceAsStream("etc/emqx_to_kafka.properties");
        Properties props = new Properties();
        try {
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //集群地址，多个服务器用"，"分隔
        props.put("bootstrap.servers", props.getProperty("kafka.cluster"));
        //重新发送消息次数，到达次数返回错误
        props.put("retries", 0);
        //Producer会尝试去把发往同一个Partition的多个Requests进行合并，batch.size指明了一次Batch合并后Requests总大小的上限。如果这个值设置的太小，可能会导致所有的Request都不进行Batch。
        props.put("batch.size", 204800);
        //Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送，以此提高吞吐量，而linger.ms则更进一步，这个参数为每次发送增加一些delay，以此来聚合更多的Message。
        props.put("linger.ms", 100);
        //在Producer端用来存放尚未发送出去的Message的缓冲区大小，默认32M
        props.put("buffer.memory", 67108864);
        //key、value的序列化，此处以字符串为例，使用kafka已有的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("partitioner.class", "com.kafka.demo.Partitioner");//分区操作，此处未写
        props.put("acks", "0");
        props.put("request.timeout.ms", "60000");
        props.put("compression.type","lz4");
        //创建生产者
        producer = new KafkaProducer<String, String>(props);

        String topic = props.getProperty("kafka.topic");
        //写入名为"emqtopic"的topic
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), msgData);
        try {
            producer.send(producerRecord).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        log.warn("Kafka producer成功写入topic:[{"+topic+"}]");
        //System.out.println("成功将数据写入Kafka");

        producer.close();
    }
}
