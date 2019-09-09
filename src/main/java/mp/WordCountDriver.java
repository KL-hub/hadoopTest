package mp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description //TODO
 * @Author 李项
 * @Date 2019/9/8
 * @Version 1.0
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //conf.set("dfs.client.use.datanode.hostname","true");
        //conf.set("fs.defaultFS", "hdfs://lixiang:8020");
        //1、获取job对象
        Job job=Job.getInstance(conf);
        //2、设置jar存储位置
        job.setJarByClass(WordCountDriver.class);
        //3、关联的Map和Reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //4、设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5、设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6、设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("/root/insertMongo.sh"));
        FileOutputFormat.setOutputPath(job,new Path("/root/testHadoop.txt"));
        //7、提交job
        job.waitForCompletion(true);
    }
}
