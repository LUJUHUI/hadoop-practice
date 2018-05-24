package inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, Text> {
	// 设置每个小文件不可分片,保证一个小文件生成一个key-value键值对
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	// 改写createRecordReader方法的逻辑，因为默认的RecordReader逻辑是获取一个LineRecordReader去逐行读取一个文本文件
	@Override
	public RecordReader<NullWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(split, context);
		// 在此返回一个我们自定义的RecordReader
		return reader;
	}
}
