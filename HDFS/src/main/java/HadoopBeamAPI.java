import java.io.IOException; // java-1.8
import java.util.List;
import java.util.ArrayList;
import org.apache.beam.sdk.Pipeline; // beam-sdks-java-io-hadoop-common
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdks.java.io.hadoop.file.system.repackaged.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.hdfs.*; // beam-sdks-java-io-hadoop-file-system
import org.apache.hadoop.conf.Configuration; // hadoop-common


public class HadoopBeamAPI {

    private static List<String> arr = new ArrayList<>();


    public static void main(String[] args) throws IOException {

        String infilename = "cmdlNotes";
        String server = "hdfs://localhost:9000";

        Configuration conf = new Configuration(); // using hadoop-hdfs dependency
        conf.set("fs.defaultFS", server);

        HadoopFileSystemOptions options = PipelineOptionsFactory.create().as(HadoopFileSystemOptions.class);

        options.setHdfsConfiguration(ImmutableList.of(conf));

        // using defaultRunner - beam-runners-direct-java dependency

        Pipeline p = Pipeline.create(options);
        // String uri = "hdfs://localhost:9000/cmdlNotes";
        p.apply(TextIO.read().from(server + '/' + infilename))
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split("[^a-zA-Z']+")) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))
                .apply(Count.<String> perElement())
                .apply("FormatResults", ParDo.of(new DoFn<KV<String, Long>, Void>() { // the return type doesn't really matter in a @ProcessElement because it is defined to have no return statement.
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        arr.add(c.element().getKey() + "\t" + c.element().getValue()); // for later print purpose
                }
                }));

        // before hitting this line, arr remains empty.
        p.run();

        System.out.println("%table word\tcount");
        arr.forEach(item -> System.out.println(item));

        // before writing to HDFS, need to make sure there is no "same file"? maybe?
        // anyway, I cannot run it twice continuously.

        System.out.println("Write to HDFS...");
        p.apply(Create.of(arr)).apply(TextIO.write().to(server + '/' + "outputFromBeam"));

        p.run();
        System.out.println("Complete.");
    }
}
