package movie;

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

public class CombinerThreeFile4OneMR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);

		job.setJarByClass(CombinerThreeFile4OneMR.class);

		job.setMapperClass(CTF4OMapper.class);
		job.setReducerClass(CTF4OReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path userInput = new Path("/hadoop/input/users.dat");
		Path movieInput = new Path("/hadoop/input/movies.dat");
		Path rateInput = new Path("/hadoop/input/ratings.dat");
		Path output = new Path("/hadoop/output/movieCount");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileInputFormat.addInputPath(job, userInput);
		FileInputFormat.addInputPath(job, movieInput);
		FileInputFormat.addInputPath(job, rateInput);
		FileOutputFormat.setOutputPath(job, output);

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}

class CTF4OMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String name = fileSplit.getPath().getName();
		String[] split = value.toString().split("::");
		if (name.equals("users.dat")) {
			String userID = split[0];
			String sex = split[1];
			String age = split[2];

			context.write(new Text(name + "-" + userID + "::" + sex + "::" + age), NullWritable.get());
		} else if (name.equals("movies.dat")) {
			String movieID = split[0];
			String movieName = split[1];
			String movieType = split[2];

			context.write(new Text(name + "-" + movieID + "::" + movieName + "::" + movieType), NullWritable.get());
		} else {
			String userID = split[0];
			String movieID = split[1];
			String rate = split[2];
			String ts = split[3];

			context.write(new Text(name + "-" + userID + "::" + movieID + "::" + rate + "::" + ts), NullWritable.get());
		}
	}
}

class CTF4OReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	List<String> userList = new ArrayList<>();
	List<String> movieList = new ArrayList<>();
	List<String> rateList = new ArrayList<>();

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		String[] split = key.toString().split("-");
		if (split[0].equals("users.dat")) {
			userList.add(split[1]);
		} else if (split[0].equals("movies.dat")) {
			movieList.equals(split[1]);
		} else {
			rateList.add(split[1]);
		}
		int userLength = userList.size();
		int movieLength = movieList.size();
		int rateLength = rateList.size();
		for (NullWritable n : values) {
			for (int i = 0; i < userLength; i++) {
				for (int j = 0; j < movieLength; j++) {
					for (int k = 0; k < rateLength; k++) {
						String keyOut = key.toString() + (userList.get(i) + "::" + movieList.get(j) + "::" + rateList.get(k));
						context.write(new Text(keyOut), NullWritable.get());
					}
				}
			}
		}
	}
}
