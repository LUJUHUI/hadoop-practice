package inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 通过自定义InputFormat合并小文件
 * 这里通过extends Configured implements Tool改写mapreduce的运行方式，是mr程序的另外一种运行方式
 */
public class SmallFilesConvertToBigMR extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SmallFilesConvertToBigMR(), args);
		System.exit(exitCode);
	}

	static class SmallFilesConvertToBigMRMapper extends Mapper<NullWritable, Text, Text, Text> {

		private Text filenameKey;

		// 在setup方法里获取filenamekey,因为setup方法是在map方法之前执行的
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}

		// 根据自定义的Inputformat的逻辑，map方法每执行一次，实际是获取到了整个文件切片的内容
		@Override
		protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
	}

	// reducer的逻辑就比较简单了， 直接原样输出就OK
	static class SmallFilesConvertToBigMRReducer extends Reducer<Text, Text, NullWritable, Text> {
		@Override
		protected void reduce(Text filename, Iterable<Text> bytes, Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), bytes.iterator().next());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		// 在创建job的时候为job指定job的名称
		Job job = Job.getInstance(conf, "combine small files to bigfile");

		job.setJarByClass(SmallFilesConvertToBigMR.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(SmallFilesConvertToBigMRMapper.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(SmallFilesConvertToBigMRReducer.class);

		// TextInputFormat是默认的数据读取组件
		// job.setInputFormatClass(TextInputFormat.class);
		// 不是用默认的读取数据的Format，我使用自定义的 WholeFileInputFormat
		job.setInputFormatClass(WholeFileInputFormat.class);

		Path input = new Path("D:\\bigdata\\smallFiles\\input");
		Path output = new Path("D:\\bigdata\\smallFiles\\output211");
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		
		FileInputFormat.setInputPaths(job, input);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		int status = job.waitForCompletion(true) ? 0 : 1;
		return status;
	}
}
