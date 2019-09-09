package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @Description //TODO
 * @Author 李项
 * @Date 2019/9/2
 * @Version 1.0
 */
public class HDFSClient {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration=new Configuration();
//        configuration.set("dfs.client.use.datanode.hostname","true");
//        configuration.set("fs.defaultFS", "hdfs://lixiang:8020");
        FileSystem fs=FileSystem.get(new URI("hdfs://www.lixiang.ac.cn:9000"),configuration,"lixiang");
        fs.mkdirs(new Path("/test"));
        fs.close();
        System.out.println("Over");
    }
}
