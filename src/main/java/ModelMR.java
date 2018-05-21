import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ModelMR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		/*配置*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		/*job实例化*/
		Job job = Job.getInstance(conf);

		job.setJarByClass(ModelMR.class);

		/*部署map-reduce组件*/
		job.setMapperClass(MRMapper.class);
		job.setReducerClass(MRReducer.class);

		/*设置map输出的k-v类型*/
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		/*最后输出的k-v格式类型*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*设置任务数*//*
		job.setNumReduceTasks(6);*/

		/*输入输出路径*/
		Path input = new Path("/hadoop/input/common-friends.txt");
		Path output = new Path("/hadoop/output/cf");
		/*去除重复的文件夹*/
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		/*判断任务是否结束*/
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);


	}
}

class MRMapper extends Mapper<LongWritable, Text, Text, Text> {

}

class MRReducer extends Reducer<Text, Text, Text, Text> {

}