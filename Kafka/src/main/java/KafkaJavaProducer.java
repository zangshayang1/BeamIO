import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by szang on 8/28/17.
 */
public class KafkaJavaProducer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;

    public KafkaJavaProducer(String topic) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
    }

    public void sampleRun() {
        int msgKey = 1;
        while (msgKey <= 10) {
            String msgVal = "Message_" + msgKey; // compose a message value
            producer.send(new ProducerRecord<Integer, String>(topic, msgKey, msgVal));
            System.out.println("Message sent: " + msgVal);
            msgKey++;
        }
    }
}
