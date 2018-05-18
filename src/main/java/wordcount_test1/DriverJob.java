package wordcount_test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置该mapreduce的运行环境所需信息
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		// 获取job对象
		Job job = Job.getInstance(conf);

		// 指定运行环境中代码的路径
		job.setJarByClass(DriverJob.class);

		job.setNumReduceTasks(3);

		// 指定Mapper和Reducer组件
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReduceClass.class);

		// 指定Mapper和Reducer组件的输出key-value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 指定该MR程序的输入输出路径
		Path input = new Path("/hadoop/input");
		Path output = new Path("/hadoop/output");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output,true);
		}

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		// 提交任务
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
