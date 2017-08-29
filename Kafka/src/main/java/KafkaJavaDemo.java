import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by szang on 8/25/17.
 */
public class KafkaJavaDemo {
    public static void main(String[] args) {

        /*
        * For producer, refer to https://github.com/apache/kafka/blob/trunk/examples/src/main/java/kafka/examples/Producer.java.
        * */

        KafkaJavaProducer myProducer = new KafkaJavaProducer("test");
        myProducer.sampleRun();

        // run on two different threads.

        KafkaJavaConsumer myConsumer = new KafkaJavaConsumer("test");
        myConsumer.listen();

    }
}
