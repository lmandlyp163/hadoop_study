package com.lm.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author
 * @create 2017-10-26 上午10:52
 * 提交到测试环境：hadoop jar  ./hdfs.study-1.0-SNAPSHOT.jar  com.lm.MR.JobTest_1
 * 注意设置文件权限
 * -DHADOOP_USER_NAME=hadoop
 **/
public class JobTest_1 {

    public static void main(String[] args) throws Exception {
        //输入路径
        String dst = "hdfs://172.16.241.134:9000/test.txt";
        //输出路径，必须是不存在的，空文件加也不行。
        String dstOut = "hdfs://172.16.241.134:9000/output/test3";
        Configuration hadoopConfig = new Configuration();

//        hadoopConfig.set("fs.hdfs.impl",
//                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//        );
//        hadoopConfig.set("fs.file.impl",
//                org.apache.hadoop.fs.LocalFileSystem.class.getName()
//        );
        Job job = Job.getInstance(hadoopConfig);

        //如果需要打成jar运行，需要下面这句
        job.setJarByClass(JobTest_1.class);

        //job执行作业时输入和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstOut));


        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
        job.setMapperClass(MapperTest_1.class);
        job.setReducerClass(ReducerTest_1.class);

        //设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //执行job，直到完成
        job.waitForCompletion(true);
        System.out.println("Finished");
    }
}
