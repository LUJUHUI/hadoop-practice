package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class test {
}

class M extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String name = fileSplit.getPath().getName();
		String[] splits = value.toString().split("::");
		if (name.equals("a.txt")) {
			String a = splits[0];
			String b = splits[1];
			String c = splits[2];
			context.write(new Text(a), new Text(name + "-" + b + c));
		} else {
			String a = splits[0];
			String d = splits[1];
			String e = splits[2];
			context.write(new Text(a), new Text(name + "-" + d + e));
		}
	}
}

class R extends Reducer<Text, Text, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> aList = new ArrayList<>();
		List<String> bList = new ArrayList<>();
		for (Text t : values) {
			String[] splits = t.toString().split("-");
			if (splits[0].equals("a.txt")) {
				aList.add(splits[1]);
			} else {
				bList.add(splits[1]);
			}
		}
		int alength = aList.size();
		int blength = bList.size();
		for (int i = 0; i < alength; i++) {
			for (int j = 0; j < blength; j++) {
				String keyout = key.toString() + "-" + (aList.get(i) + "," + bList.get(j));
				context.write(new Text(keyout), NullWritable.get());
			}
		}

	}
}