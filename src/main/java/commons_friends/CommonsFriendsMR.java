package commons_friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CommonsFriendsMR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*--------------conf1------------------*/
		/*配置*/
		Configuration conf1 = new Configuration();
		conf1.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		/*job实例化*/
		Job job = Job.getInstance(conf1);
		job.setJarByClass(CommonsFriendsMR.class);
		/*部署map-reduce组件*/
		job.setMapperClass(CommonsFriendsMRMapper.class);
		job.setReducerClass(CommonsFriendsMRReducer.class);
		/*设置map输出的k-v类型*/
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		/*最后输出的k-v格式类型*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		/*输入输出路径*/
		Path input = new Path("/hadoop/input/common-friends.txt");
		Path output = new Path("/hadoop/output/cf");
		/*去除重复的文件夹*/
		FileSystem fs = FileSystem.get(conf1);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		/*--------------conf2------------------*/
		Configuration conf2 = new Configuration();
		conf2.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		/*job实例化*/
		Job job1 = Job.getInstance(conf2);
		job1.setJarByClass(CommonsFriendsMR.class);
		/*部署map-reduce组件*/
		job1.setMapperClass(CommonFriendMRMapper2.class);
		job1.setReducerClass(CommonFriendMRReducer2.class);
		/*设置map输出的k-v类型*/
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		/*最后输出的k-v格式类型*/
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		/*输入输出路径*/
		Path input1 = new Path("/hadoop/output/cf/part-r-00000");
		Path output1 = new Path("/hadoop/output/cf1");
		/*去除重复的文件夹*/
		FileSystem fs1 = FileSystem.get(conf2);
		if (fs1.exists(output1)) {
			fs1.delete(output1, true);
		}
		FileInputFormat.setInputPaths(job1, input1);
		FileOutputFormat.setOutputPath(job1, output1);

		/*-------------JobControl-------------*/
		ControlledJob step1CF = new ControlledJob(conf1);
		ControlledJob step2CF = new ControlledJob(conf2);
		step1CF.setJob(job);
		step2CF.setJob(job1);

		/*设置任务间的依赖关系*/
		step2CF.addDependingJob(step1CF);

		/*通过JobControl去管理一组有依赖关系的任务的执行*/
		JobControl cf = new JobControl("CF");
		cf.addJob(step1CF);
		cf.addJob(step2CF);

		/*提交任务*/
		Thread thread = new Thread(cf);
		thread.start();

		/*判断任务执行的完成情况*/
		while (!cf.allFinished()) {
			Thread.sleep(3000);
		}

		/*关闭任务线程*/
		cf.stop();

	}
}

class CommonsFriendsMRMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(":");
		String user = split[0];  //A
		String friendStr = split[1];//b,c,d
		String[] friends = friendStr.split(",");
		for (String friend : friends) {
			context.write(new Text(friend), new Text(user)); /*<b,A>;<c,A>;.....*/
		}
	}
}

class CommonsFriendsMRReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();//A-B-C-D-E-
		for (Text t : values) {
			sb.append(t.toString()).append("-");//A-
		}
		String friend = sb.toString().substring(0, sb.toString().length() - 1);//A-B-C-D-E
		context.write(key, new Text(friend));/*<b,A-B-C-D-E>;<c,D-E--F-T>*/
	}
}


class CommonFriendMRMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String user = split[0];// b
		String friends = split[1];//A-B-C-D-E
		String[] friendStr = friends.split("-");//{"A","D","C","B","E"....}

		/*避免出现AB  BA的情况，因此做一下排序*/
		Arrays.sort(friendStr);//{"A","B","C","D","E"....}

		for (int i = 0; i < friendStr.length - 1; i++) { //i=0
			for (int j = i + 1; j < friendStr.length; j++) {//j=1
				context.write(new Text(friendStr[i] + "-" + friendStr[j]), new Text(user));/*<A-B-..,b>*/
			}
		}
	}
}

class CommonFriendMRReducer2 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for (Text t : values) {
			sb.append(t.toString()).append(",");
		}
		String friend = sb.toString().substring(0, sb.toString().length() - 1);

		context.write(key, new Text(friend));/*<b-d,E-C>*/
	}
}