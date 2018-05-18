package flow.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProvinceParMap extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split("\t");

		FlowBean k = new FlowBean();
		k.setTelphone(lines[1]);
		k.setUpFlow(Long.parseLong(lines[lines.length - 3]));
		k.setDownFlow(Long.parseLong(lines[lines.length - 2]));
		k.setSumFlow(Long.parseLong(lines[lines.length - 3])+Long.parseLong(lines[lines.length - 2]));

		/*写出*/
		context.write(k, NullWritable.get());
	}
}
