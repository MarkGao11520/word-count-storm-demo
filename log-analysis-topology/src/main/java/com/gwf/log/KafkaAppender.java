package com.gwf.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.gwf.log.formatter.Formatter;
import com.gwf.log.formatter.MessageFormatter;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


@Data
public class KafkaAppender extends AppenderBase<ILoggingEvent> {

    private String topic;
    private String zookeeperHost;
    private Producer<String,String> producer;
    private Formatter formatter;

    @Override
    public void start() {
        if(null == this.formatter){
            this.formatter = new MessageFormatter();
        }
        super.start();
        Properties properties = new Properties();
        properties.put("zk.connect",this.zookeeperHost);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
//        ProducerConfig config = new ProducerConfig(properties);
 //       this.producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void stop() {
        super.stop();
        this.producer.close();
    }

    @Override
    protected void append(ILoggingEvent iLoggingEvent) {
        String payload = this.formatter.format(iLoggingEvent);
        ProducerRecord data = new ProducerRecord(topic,payload);
//        this.producer.send(data);
        System.out.println(data);
        System.out.println(payload);
        System.out.println(topic);

    }
}
