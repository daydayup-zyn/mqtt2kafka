package com.zhbr.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.Properties;
import java.util.UUID;

/**
 * @ClassName App
 * @Description TODO
 * @Autor yanni
 * @Date 2020/9/2 14:03
 * @Version 1.0
 **/
public class App {

    private static Logger log = Logger.getLogger(App.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        try {
            InputStream in = MqttKafkaProducer.class.getClassLoader().getResourceAsStream("etc/emqx_to_kafka.properties");
//            // 使用InPutStream流读取properties文
//            BufferedReader bufferedReader = new BufferedReader(new FileReader(args[0]));
//            // 使用 properties 对象加载输入流
           properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String subTopic = properties.getProperty("emqx.topic");
        String broker = properties.getProperty("emqx.broker");
        String clientId = UUID.randomUUID().toString();
        MemoryPersistence persistence = new MemoryPersistence();

        try {

            MqttClient client = new MqttClient(broker, clientId, persistence);

            // MQTT 连接选项
            MqttConnectOptions connOpts = new MqttConnectOptions();
            //connOpts.setUserName("emqx_test");
            //connOpts.setPassword("emqx_test_password".toCharArray());
            // 保留会话
            connOpts.setCleanSession(true);

            // 设置回调
            client.setCallback(new OnMessageCallback());

            // 建立连接
            log.warn("clientId: {"+clientId+"} is connecting to broker: {"+broker+"}");
            client.connect(connOpts);

            log.warn("clientId: {"+clientId+"} has Connected");
            //System.out.println("Publishing message: " + content);

            // 订阅
            client.subscribe(subTopic);

            // 消息发布所需参数
//            MqttMessage message = new MqttMessage(content.getBytes());
//            message.setQos(qos);
//            client.publish(pubTopic, message);
//            System.out.println("Message published");
//
//            client.disconnect();
//            System.out.println("Disconnected");
//            client.close();
//            System.exit(0);
        } catch (MqttException me) {
            log.error("reason " + me.getReasonCode());
            log.error("msg " + me.getMessage());
            log.error("loc " + me.getLocalizedMessage());
            log.error("cause " + me.getCause());
            log.error("excep " + me);
            me.printStackTrace();
        }
    }
}
