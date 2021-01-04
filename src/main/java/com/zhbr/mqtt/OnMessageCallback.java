package com.zhbr.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName OnMessageCallback
 * @Description TODO
 * @Autor yanni
 * @Date 2020/9/2 14:05
 * @Version 1.0
 **/
public class OnMessageCallback implements MqttCallback{
    private Logger log = Logger.getLogger(OnMessageCallback.class);
    public void connectionLost(Throwable cause) {
        // 连接丢失后，一般在这里面进行重连
        log.error("client连接断开，可以做重连");
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        // subscribe后得到的消息会执行到这里面
        log.warn("EMQ client接收到EMQ消息主题:[{"+topic+"}]");
//        log.info("EMQ client接收消息Qos:{}" , message.getQos());
//        log.info("EMQ client接收消息内容:{}" , new String(message.getPayload()));

        MqttKafkaProducer.pushData(new String(message.getPayload()));
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
        log.warn("EMQ client deliveryComplete---------" + token.isComplete());
    }
}
