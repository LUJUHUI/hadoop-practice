package inputformat;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MergeSmallFile2BigMR extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(conf);

		job.setJarByClass(MergeSmallFile2BigMR.class);

		job.setInputFormatClass(MergeInputFormat.class);

		job.setMapperClass(MergeSmallFile2BigMRMapper.class);
		job.setReducerClass(MergeSmallFile2BigMRReducer.class);
//		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path outPutPath = new Path("/hadoop/newInput");
		FileInputFormat.addInputPath(job, new Path("/hadoop/input/score.txt"));
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPutPath)) {
			fs.delete(outPutPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPutPath);

		boolean waitForCompletion = job.waitForCompletion(true);
		return waitForCompletion ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MergeSmallFile2BigMR(), args);
		System.exit(exitCode);
	}

	/**
	 * MergeInputFormat:一次读入一个完整文件，所有的数值都封装在Text（key-value当中的value）
	 */
	public static class MergeInputFormat extends FileInputFormat<LongWritable, Text> {

		@Override
		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

//			FileSplit fileSplit = ((FileSplit) split);
//			Path path = fileSplit.getPath();
//			long splitLength = fileSplit.getLength();
//
//			FileSystem fs = FileSystem.get(context.getConfiguration());
//			FSDataInputStream open = fs.open(path);

			MyRecordReader mrr = new MyRecordReader();
			mrr.initialize(split, context);
			return mrr;
		}

		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}
	}

	public static class MyRecordReader extends RecordReader<LongWritable, Text> {

		private FSDataInputStream open;
		private FileSplit fileSplit;
		private Configuration conf;
		private FileSystem fs;

		private boolean progress = false;

		private LongWritable key = new LongWritable();
		private Text value = new Text();

		/*public MyRecordReader(FSDataInputStream open, long length) {
			this.open = open;
			this.splitLength = length;
		}*/

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			this.fileSplit = (FileSplit) split;
			this.conf = context.getConfiguration();
			this.fs = FileSystem.get(conf);
		}

		/**
		 * 需求：一次读入一个文件切片的所有数据
		 */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!progress) {
				Path path = fileSplit.getPath();
				open = fs.open(path);
				byte[] content = new byte[(int) fileSplit.getLength()];
				IOUtils.readFully(open, content, 0, (int) fileSplit.getLength());
				key.set(0L);
				value.set(content, 0, content.length);
				progress = true;
				return true;
			}
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return progress ? 1f : 0f;
		}

		@Override
		public void close() throws IOException {
			open.close();
		}
	}

	public static class MergeSmallFile2BigMRMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			context.write(value, NullWritable.get());
		}
	}

	public static class MergeSmallFile2BigMRReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			for (NullWritable nvl : values) {
				context.write(key, NullWritable.get());
			}
		}
	}
}
