import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szang on 8/23/17.
 */
public class HadoopJavaIO {

    public static void main(String[] args) throws IOException {

        String server = "hdfs://localhost:9000";
        String infilename = "cmdlNotes";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

/*
    Security Loggin Reference: https://github.corp.ebay.com/DSS/tora-framework/blob/master/toracommon-dao-parent/toracommon-dao-hbase/src/main/java/com/ebay/dss/toracommon/dao/hdfs/HdfsDataSourceConnection.java

        conf.set("hadoop.security.authentication", "kerberos");
        String user = "sg_adm@CORP.EBAY.COM";
        String keytab = "/Users/szang/Desktop/MingMin/.authentication/sg_adm.keytab";
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(user, keytab);
*/
        System.out.println("Reading file...");
        FileSystem readSesh = FileSystem.get(URI.create(server), conf);
        FSDataInputStream inputstream = readSesh.open(new Path(server + '/' + infilename));
        BufferedReader br = new BufferedReader(new InputStreamReader(inputstream));
        String line;
        List<String> content = new ArrayList<>();
        line = br.readLine();
        while (line != null) {
            System.out.println("Reading:" + line);
            content.add(line);
            line = br.readLine();
        }
        readSesh.close();

        /*
            You cannot read from and write to the same session at the same run.
        */

        FileSystem writeSesh = FileSystem.get(URI.create(server), conf);
        System.out.println("Writing file...");
        String outfilename = "output";
        Path path = new Path(server + '/' + outfilename);
        if (writeSesh.exists(path)) {
            writeSesh.delete(path, true); // recursive = true
        }
        FSDataOutputStream outputStream = writeSesh.create(path);
        for (int i = 0; i < content.size(); i++) {
            outputStream.writeBytes(content.get(i) + '\n');
        }
        outputStream.close();
    }
}
