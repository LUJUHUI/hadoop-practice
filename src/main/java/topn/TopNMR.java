package topn;


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

public class TopNMR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);

		job.setJarByClass(TopNMR.class);

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);

		job.setMapOutputKeyClass(CourseScore.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(CourseScore.class);
		job.setOutputValueClass(NullWritable.class);

		job.setGroupingComparatorClass(CourseScoreComparator.class);

		Path input = new Path("/hadoop/input/score.txt");
		Path output = new Path("/hadoop/output/topN");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 1 : 0);

	}
}

/*用户自定义分组规则*/
class CourseScoreComparator extends WritableComparator {
	CourseScoreComparator() {
		super(CourseScore.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CourseScore csa = (CourseScore) a;
		CourseScore csb = (CourseScore) b;
		int compareTo = csa.getCourse().compareTo(csb.getCourse());
		return compareTo;
	}
}

class TopNMapper extends Mapper<LongWritable, Text, CourseScore, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		String course = split[0];
		String name = split[1];

		int avgScore = 0;
		int sumScore = 0;
		int sum = 0;

		for (int i = 2; i < split.length; i++) {
			if (!StringUtil.isBlank(split[i])) {
				sum++;
				sumScore += Double.parseDouble(split[i]);
			}
		}
		avgScore = sumScore / sum;

		CourseScore cs = new CourseScore(course, name, avgScore);
		context.write(cs, NullWritable.get());//<computer,lili,90>
	}
}

/*
 * reduce求每一门课程的最高分
 * */
class TopNReducer extends Reducer<CourseScore, NullWritable, CourseScore, NullWritable> {
	/*设定求topN*/
	int topN = 1;

	@Override
	protected void reduce(CourseScore key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		/*top1
		context.write(key, NullWritable.get());*/

		/*top2*/
		int counter = 0;
		for (NullWritable nvl : values) {
			context.write(key, NullWritable.get());
			counter++;
			if (counter == topN) {
				break;
			}
		}
	}
}