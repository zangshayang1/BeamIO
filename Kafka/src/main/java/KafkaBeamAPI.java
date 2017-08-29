import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by szang on 8/24/17.
 */
public class KafkaBeamAPI {


    public static void main(String[] args) throws IOException {

        PipelineOptions options = PipelineOptionsFactory.create().as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        Map<Integer, String> hm = new HashMap<>();
        hm.put(1, "Message_100");
        hm.put(2, "Message_200");
        hm.put(3, "Message_300");

        PCollection<KV<Integer, String>> sampleData = p.apply(Create.of(hm));

        sampleData.apply(KafkaIO.<Integer, String>write()
                .withBootstrapServers("localhost:9092")
                .withTopic("test")
                .withKeySerializer(IntegerSerializer.class)
                .withValueSerializer(StringSerializer.class)
        );


        p.apply(KafkaIO.<Integer, String>read()
         .withBootstrapServers("localhost:9092")
         .withTopic("test")
         .withKeyDeserializer(IntegerDeserializer.class)
         .withValueDeserializer(StringDeserializer.class)
         .withoutMetadata()
        )
         .apply("PrintMessages", ParDo.of(new DoFn<KV<Integer, String>, Void>() {
             @ProcessElement
             public void processElement(ProcessContext c) {
                 System.out.println("[key]: " + c.element().getKey() + "[value]: " + c.element().getValue());
             }
         }));

        p.run();
    }
}
