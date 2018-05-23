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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinMR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*配置*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		/*job实例化*/
		Job job = Job.getInstance(conf);

		job.setJarByClass(JoinMR.class);

		/*部署map-reduce组件*/
		job.setMapperClass(JoinMRMapper.class);
		job.setReducerClass(JoinMRReducer.class);

		/*设置map输出的k-v类型*/
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		/*最后输出的k-v格式类型*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		/*设置任务数*//*
		job.setNumReduceTasks(6);*/

		/*输入输出路径*/
		Path inputUser = new Path("/hadoop/input/user.log");
		Path inputGoods = new Path("/hadoop/input/goods.log");
		Path output = new Path("/hadoop/output/user_goods");
		/*去除重复的文件夹*/
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		/*FileInputFormat.addInputPath兼容FileInputFormat.setInputPath,通常用addInputPath*/
		FileInputFormat.addInputPath(job, inputUser);
		FileInputFormat.addInputPath(job, inputGoods);
		FileOutputFormat.setOutputPath(job, output);

		/*判断任务是否结束*/
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}

class JoinMRMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*获取文件切片*/
		FileSplit fileSplit = (FileSplit) context.getInputSplit();

		/*获取文件的文件名*/
		String name = fileSplit.getPath().getName();
		String[] splits = value.toString().split(",");
		/*判断获取的文件切片是属于user.log文件还是goods.log文件的*/
		if (name.equals("user.log")) {
			String userID = splits[0];
			String userName = splits[1];
			String userAge = splits[2];
			/*此处value拼接上name是为了在reduce阶段进行区分文件所属哪个log*/
			context.write(new Text(userID), new Text(name + "-" + userName + "," + userAge));
		} else {
			String goodID = splits[0];
			String userID = splits[1];
			String goodPrice = splits[2];
			String ts = splits[3];
			context.write(new Text(userID), new Text(name + "-" + goodID + "," + goodPrice + "," + ts));
		}
	}
}

class JoinMRReducer extends Reducer<Text, Text, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> userList = new ArrayList<>();
		List<String> goodList = new ArrayList<>();
		for (Text t : values) {
			/*获取name,根据name判断得到的切片属于哪个文件，然后添加到对应的list列表中*/
			String[] splits = t.toString().split("-");
			if (splits[0].equals("user.log")) {
				userList.add(splits[1]);
			} else {
				goodList.add(splits[1]);
			}
		}
		/*获取列表长度*/
		int userLength = userList.size();
		int goodLength = goodList.size();
		for (int i = 0; i < userLength; i++) {
			for (int j = 0; j < goodLength; j++) {
				/*key值为用户ID，按照循环将两张表进行join*/
				String keyout = key.toString() + "," + (userList.get(i) + "," + goodList.get(j));
				context.write(new Text(keyout), NullWritable.get());
			}
		}
	}
}