package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoin {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		/*配置*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		/*job实例化*/
		Job job = Job.getInstance(conf);

		job.setJarByClass(ReduceJoin.class);

		/*部署map-reduce组件*/
		job.setMapperClass(ReduceJoinMRMapper.class);
		job.setReducerClass(ReduceJoinMRReducer.class);

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

class ReduceJoinMRMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/*获取文件切片*/
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		/*判断获取的文件切片是属于movies.dat文件还是ratings.dat文件的*/
		String name = fileSplit.getPath().getName();
		String[] splits = value.toString().split("::");
		if (name.equals("movies.dat")) {
			String movieID = splits[0];
			String movieName = splits[1];
			String movieType = splits[2];
			context.write(new Text(movieID), new Text(name + "-" + movieName + "::" + movieType));

		} else {
			String userID = splits[0];
			String movieID = splits[1];
			String rate = splits[2];
			String ts = splits[3];
			context.write(new Text(movieID), new Text(name + "-" + userID + "::" + rate + "::" + ts));
		}
	}
}

class ReduceJoinMRReducer extends Reducer<Text, Text, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> movieList = new ArrayList<>();
		List<String> ratingList = new ArrayList<>();
		for (Text t : values) {
			String[] splits = t.toString().split("-");
			if (splits[0].equals("movies.dat")) {
				movieList.add(splits[1]);
			} else {
				ratingList.add(splits[1]);
			}
		}

		int movieLength = movieList.size();
		int ratingLength = ratingList.size();
		for (int i = 0; i < movieLength; i++) {
			for (int j = 0; j < ratingLength; j++) {
				String keyout = key.toString() + "::" + (ratingList.get(j) + "::" + movieList.get(i));
				context.write(new Text(keyout), NullWritable.get());
			}
		}
	}
}
