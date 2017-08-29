import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by szang on 8/28/17.
 */
public class KafkaJavaConsumer extends Thread {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public KafkaJavaConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<Integer, String>(props);
        this.topic = topic;
    }

    public void listen() {
        consumer.subscribe(Collections.singletonList(this.topic));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(5000);
            for (ConsumerRecord record : records) {
                System.out.println("Received: " + "[key]:" + record.key() + "[value]:" + record.value());
            }
        }
    }
}
