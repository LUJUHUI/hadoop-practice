package fan;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HuFenMRMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(":");
		String[] fans = split[1].split(",");
		for (String fan : fans) {
			/*剔除AB、BA这种情况的发生*/
			String keyOut = split[0].compareTo(fan) < 0 ? (split[0] + " " + fan) : (fan + " " + split[0]);
			String valueOut = fan;

			context.write(new Text(keyOut), new Text(valueOut));
		}
	}
}
