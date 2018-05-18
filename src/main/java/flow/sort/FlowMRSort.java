package flow.sort;

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

public class FlowMRSort {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowMRSort.class);

		job.setMapperClass(FlowSortMap.class);
		job.setReducerClass(FlowSortReduce.class);

		job.setMapOutputKeyClass(FlowBeanSort.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBeanSort.class);

		Path input = new Path("/hadoop/input/flow.log");
		Path output = new Path("/hadoop/output/flowsort");
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

class FlowSortMap extends Mapper<LongWritable, Text, FlowBeanSort, Text> {
	FlowBeanSort k = new FlowBeanSort();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split("\t");

		String telphoneNum = lines[1];
		long upFlow = Long.parseLong(lines[lines.length - 3]);
		long downFlow = Long.parseLong(lines[lines.length - 2]);

		k.set(upFlow, downFlow);
		v.set("手机号:"+telphoneNum);

		context.write(k, v);
	}
}

class FlowSortReduce extends Reducer<FlowBeanSort, Text, Text, FlowBeanSort> {
	@Override
	protected void reduce(FlowBeanSort bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(values.iterator().next(), bean);
	}
}
