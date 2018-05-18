package flow.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProvinceDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);

		job.setJarByClass(ProvinceDriver.class);

		job.setMapperClass(ProvinceParMap.class);
		job.setReducerClass(ProvinceParReduce.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);

		/*按电话号码分区*/
		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(6);

		Path input = new Path("/hadoop/input/flow.log");
		Path output = new Path("/hadoop/output/partitioner");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileInputFormat.setInputPaths(job,input);
		FileOutputFormat.setOutputPath(job,output);

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}
