package flow.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lujuhui.
 * @date 2018/5/18.
 */

public class test {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*配置*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		/*job实例化*/
		Job job = Job.getInstance(conf);

		job.setJarByClass(test.class);

		/*部署map-reduce组件*/
		job.setMapperClass(testMap.class);
		job.setReducerClass(testReduce.class);

		/*设置map输出的k-v类型*/
		job.setMapOutputKeyClass(FlowBean1.class);
		job.setMapOutputValueClass(NullWritable.class);

		/*最后输出的k-v格式类型*/
		job.setOutputKeyClass(FlowBean1.class);
		job.setMapOutputValueClass(NullWritable.class);

		/*分区shuffer*/
		job.setPartitionerClass(ProvincePaartitioner.class);

		/*设置任务数*/
		job.setNumReduceTasks(6);

		/*输入输出路径*/
		Path input = new Path("/hadoop/input/flow.log");
		Path output = new Path("/hadoop/test/partitioner");
		/*去除重复的文件夹*/
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		/*判断任务是否结束*/
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}

/*map导入，取得需要字段*/
class testMap extends Mapper<LongWritable, Text, FlowBean1, NullWritable> {
	FlowBean1 k = new FlowBean1();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split("\t");

		k.setTelphoneNum(lines[1]);
		k.setUpFlow(lines.length - 3);
		k.setDownFlow(lines.length - 2);

		context.write(k, NullWritable.get());
	}
}

/*reducer去重*/
class testReduce extends Reducer<FlowBean1, NullWritable, FlowBean1, NullWritable> {
	@Override
	protected void reduce(FlowBean1 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}

/*分区*/
class ProvincePaartitioner extends Partitioner<FlowBean1, NullWritable> {
	private static Map<String, Integer> promap = new HashMap<>();

	static {
		promap.put("135", 0);
		promap.put("136", 1);
		promap.put("159", 2);
		promap.put("134", 3);
		promap.put("138", 4);
	}

	@Override
	public int getPartition(FlowBean1 key, NullWritable value, int numPartitions) {
		String telphone = key.getTelphoneNum();
		String prefixthree = telphone.substring(0, 3);
		if (!promap.containsKey(prefixthree)) {
			return promap.get(prefixthree);
		} else {
			return 5;
		}
	}
}

/*bean*/
class FlowBean1 implements WritableComparable<FlowBean1> {
	private String telphoneNum;
	private long upFlow;
	private long downFlow;
	private long sumFlow;

	public FlowBean1() {
	}

	public void set(String telphoneNum, long upFlow, long downFlow) {
		this.telphoneNum = telphoneNum;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	public String getTelphoneNum() {
		return telphoneNum;
	}

	public void setTelphoneNum(String telphoneNum) {
		this.telphoneNum = telphoneNum;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	/*存储格式*/
	@Override
	public String toString() {
		return "手机号码:" + telphoneNum + '\t' + "上行流量" + upFlow + "\t" + "下行流量:" + downFlow + "\t" + "总流量:" + sumFlow;
	}

	/*反序列化*/
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telphoneNum);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	/*序列化*/
	@Override
	public void readFields(DataInput in) throws IOException {
		this.telphoneNum = in.readUTF();
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	/*sort*/
	@Override
	public int compareTo(FlowBean1 fb) {
		int result = (int) (fb.sumFlow - this.sumFlow);
		if (result > 0) {
			return 1;
		} else if (result < 0) {
			return -1;
		} else {
			return 0;
		}
	}
}