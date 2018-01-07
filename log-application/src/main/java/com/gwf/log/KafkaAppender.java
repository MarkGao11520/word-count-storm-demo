package com.gwf.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.gwf.log.formatter.Formatter;
import com.gwf.log.formatter.MessageFormatter;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.Data;


import java.util.Properties;


@Data
public class KafkaAppender extends AppenderBase<ILoggingEvent> {

    private String topic;
    private String zookeeperHost;
    private String brokerList;
    private Producer<String,String> producer;
    private Formatter formatter;

    @Override
    public void start() {
        if(null == this.formatter){
            this.formatter = new MessageFormatter();
        }
        super.start();
        Properties properties = new Properties();
        properties.put("metadata.broker.list",brokerList);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");
        ProducerConfig config = new ProducerConfig(properties);
        this.producer = new Producer<String, String>(config);
    }

    @Override
    public void stop() {
        super.stop();
        this.producer.close();
    }

    @Override
    protected void append(ILoggingEvent iLoggingEvent) {
        String payload = this.formatter.format(iLoggingEvent);
        producer.send(new KeyedMessage<String, String>(topic,payload));
        System.out.println("kafka sent: "+ payload);
    }
}
