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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		/*配置*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		/*job实例化*/
		Job job = Job.getInstance(conf);

		job.setJarByClass(MapJoinMR.class);

		/*部署map-reduce组件*/
		job.setMapperClass(MapJoinMRMapper.class);
		//job.setReducerClass(MapJoinMRReducer.class);

		job.setNumReduceTasks(0);

		/*设置map输出的k-v类型*/
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		/*最后输出的k-v格式类型*/
		/*job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);*/

		/*Path inputMovie = new Path("/hadoop/input/movies.dat");*/
		URI uri = new URI("hdfs://hadoop01:9000/hadoop/input/movies.dat");
		URI[] uriArray = new URI[]{uri};
		job.setCacheFiles(uriArray);

		/*输入输出路径*/

		Path inputRating = new Path("hdfs://hadoop01:9000/hadoop/input/ratings.dat");
		Path output = new Path("hdfs://hadoop01:9000/hadoop/output/movie2");
		/*去除重复的文件夹*/
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		/*FileInputFormat.addInputPath兼容FileInputFormat.setInputPath,通常用addInputPath*/
		/*FileInputFormat.addInputPath(job, inputMovie);*/
		FileInputFormat.addInputPath(job, inputRating);
		FileOutputFormat.setOutputPath(job, output);

		/*判断任务是否结束*/
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}

class MapJoinMRMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private static Map<String, String> movieMap = new HashMap<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		/*URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].toURL().getPath();*/

		Path[] localCacheFiles = context.getLocalCacheFiles();
		String strPath = localCacheFiles[0].toUri().toString();
		BufferedReader br = new BufferedReader(new FileReader(strPath));
		String line = null;
		if (null != (line = br.readLine())) {
			String[] split = line.toString().split("::");
			String movieID = split[0];
			String movieName = split[1];
			String movieType = split[2];
			movieMap.put(movieID, movieName + "::" + movieType);
		}
		br.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split("::");
		String userID = splits[0];
		String movieID = splits[1];
		String rate = splits[2];
		String ts = splits[3];

		String mapStr = movieMap.get(movieID);
		String keyOut = movieID + "::" + userID + "::" + rate + "::" + ts + "::" + mapStr;

		context.write(new Text(keyOut), NullWritable.get());

	}
}
