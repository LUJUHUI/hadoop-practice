package flow.count;

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

public class FlowMRSum {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowMRSum.class);

		job.setMapperClass(FlowMap.class);
		job.setReducerClass(FlowMRCombiner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		Path input = new Path("/hadoop/input/flow.log");
		Path output = new Path("/hadoop/output/flowcount");
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

class FlowMap extends Mapper<LongWritable, Text, Text, FlowBean> {
	Text k = new Text();
	FlowBean v = new FlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*1.通过对读取的数据进行切割，获取所需数据*/
		String[] lines = value.toString().split("\t");
		String telphoneNum = lines[1];
		long upFlow = Long.parseLong(lines[lines.length - 3]);
		long downFlow = Long.parseLong(lines[lines.length - 2]);

		/*2.设置输出的k-v对*/
		k.set("手机号:"+telphoneNum);
		v.set(upFlow, downFlow);

		/*3.写出的k-v对*/
		context.write(k, v);
	}
}

class FlowMRCombiner extends Reducer<Text, FlowBean, Text, FlowBean> {
	FlowBean v = new FlowBean();

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
		long upFlowCount = 0;
		long downFlowCount = 0;

		for (FlowBean bean : values) {
			upFlowCount += bean.getUpFlow();
			downFlowCount += bean.getDownFlow();
		}
		v.set(/*key.toString(), */upFlowCount, downFlowCount);
		context.write(key, v);
	}
}