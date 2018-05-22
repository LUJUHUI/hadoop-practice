package topn.test;

import jodd.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TN {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);

		job.setGroupingComparatorClass(CourseCP.class);

		job.setJarByClass(TN.class);

		job.setMapperClass(TNMapper.class);
		job.setReducerClass(TNReducer.class);

		job.setMapOutputKeyClass(CourseBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(CourseBean.class);
		job.setOutputValueClass(NullWritable.class);

		Path input = new Path("/hadoop/input/score.txt");
		Path output = new Path("/hadoop/output/topN");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}

class CourseCP extends WritableComparator {
	public CourseCP() {
		super(CourseBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CourseBean csa = (CourseBean) a;
		CourseBean csb = (CourseBean) b;

		int compareTo = csa.getCourse().compareTo(csb.getCourse());
		return compareTo;
	}
}

class TNMapper extends Mapper<LongWritable, Text, CourseBean, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String course = split[0];
		String name = split[1];

		int sum = 0;
		int avgScore = 0;
		int sumScore = 0;
		for (int i = 2; i < split.length; i++) {
			if (!StringUtil.isBlank(split[i])) {
				sum++;
				sumScore += Double.parseDouble(split[i]);
			}
		}
		avgScore = sumScore / sum;

		CourseBean cs = new CourseBean(course, name, avgScore);
		context.write(cs, NullWritable.get());
	}
}

class TNReducer extends Reducer<CourseBean, NullWritable, CourseBean, NullWritable> {
	int topN = 2;

	@Override
	protected void reduce(CourseBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		int counter = 0;
		for (NullWritable nvl : values) {
			counter++;
			context.write(key, NullWritable.get());
			if (counter == topN) {
				break;
			}
		}
	}
}