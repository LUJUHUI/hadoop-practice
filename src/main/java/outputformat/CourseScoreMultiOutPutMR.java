package outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CourseScoreMultiOutPutMR extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME","hadoop");

		Job job = Job.getInstance(conf);

		job.setJarByClass(CourseScoreMultiOutPutMR.class);

		job.setMapperClass(CourseScoreMultiOutPutMRMapper.class);
		job.setReducerClass(CourseScoreMultiOutPutMRReducer.class);

		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(CSMultiOutputFormat.class);
		Path input = new Path("/hadoop/input/score.txt");
		Path output = new Path("/hadoop/output/1");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		boolean b = job.waitForCompletion(true);
		return b ? 0 : 1;
	}

	public static class CSMultiOutputFormat extends TextOutputFormat<Text, NullWritable> {
		@Override
		public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
			Configuration configuration = job.getConfiguration();
			FileSystem fileSystem = FileSystem.get(configuration);
			/*if(fileSystem.exists(new Path("/hadoop/outComputer"))){
				fileSystem.delete((new Path("/hadoop/outComputer")),true);
			}else if(fileSystem.exists(new Path("/hadoop/outMath"))){
				fileSystem.delete(new Path("/hadoop/outMath"),true);
			}*/
			FSDataOutputStream outComputer = fileSystem.create(new Path("/hadoop/newOutPut/outputComputer"));
			FSDataOutputStream outMath = fileSystem.create(new Path("/hadoop/newOutPut/outputMath"));

			return new MyRecordWriter(outComputer,outMath);
		}
	}

	public static class MyRecordWriter extends RecordWriter<Text, NullWritable> {
		FSDataOutputStream outComputer;
		FSDataOutputStream outMath;

		public MyRecordWriter(FSDataOutputStream outComputer,FSDataOutputStream outMath) {
			this.outComputer = outComputer;
			this.outMath = outMath;
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			if (key.toString().indexOf("computer") != -1) {
				outComputer.write(key.toString().getBytes());
				outComputer.write("\n".getBytes("UTF-8"));
			}else if(key.toString().indexOf("math") != -1){
				outMath.write(key.toString().getBytes());
				outMath.write("\n".getBytes("UTF-8"));
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			outComputer.close();
			outMath.close();
		}
	}

	public static class CourseScoreMultiOutPutMRMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}

	public static class CourseScoreMultiOutPutMRReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new CourseScoreMultiOutPutMR(), args);
		System.exit(exitcode);
	}
}
