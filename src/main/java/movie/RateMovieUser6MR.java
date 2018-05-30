package movie;

import join.ReduceJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RateMovieUser6MR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		/*配置*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		/*job实例化*/
		Job job = Job.getInstance(conf);

		job.setJarByClass(RateMovieUser6MR.class);

		/*部署map-reduce组件*/
		job.setMapperClass(RateMovieUser6MRStep1Mapper.class);
		job.setReducerClass(RateMovieUser6MRStep1Reducer.class);

		/*job.setMapperClass(RateMovieUser6MRStep2Mapper.class);
		job.setReducerClass(RateMovieUser6MRStep2Reducer.class);*/

		/*设置map输出的k-v类型*/
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		/*最后输出的k-v格式类型*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		/*设置任务数*//*
		job.setNumReduceTasks(6);*/

		/*输入输出路径*/
		Path inputMovie = new Path("/hadoop/input/movies.dat");
		Path inputRating = new Path("/hadoop/input/ratings.dat");
		Path output = new Path("/hadoop/output/movieCount");
		/*去除重复的文件夹*/
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		/*FileInputFormat.addInputPath兼容FileInputFormat.setInputPath,通常用addInputPath*/
		FileInputFormat.addInputPath(job, inputMovie);
		FileInputFormat.addInputPath(job, inputRating);
		FileOutputFormat.setOutputPath(job, output);

		/*判断任务是否结束*/
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
