package inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class WholeFileRecordReader extends RecordReader<NullWritable, Text> {
	private FileSplit fileSplit;
	private Configuration conf;
	private Text value = new Text();
	private boolean processed = false;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}

	// nextKeyValue()方法是RecordReader最重要的方法，也就是RecordReader读取文件的读取逻辑所在地
	// 所以我们要自定义RecordReader，就需要重写nextKeyValue()的实现
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			// 创建一个输入切片的字节数组，用来存储将要读取的数据内容
			byte[] contents = new byte[(int) fileSplit.getLength()];
			// 通过 filesplit获取该逻辑切片在文件系统的位置
			Path file = fileSplit.getPath();
			// 通过该file对象获取该切片所在的文件系统
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				// 文件系统对象fs打开一个file的输入流
				in = fs.open(file);
				// in是输入流 contents是存这个流读取的到数的数据的字节数组
				// 从输入流in上从起始偏移量0开始读取contents.length长度的数据到contents，实际上也就是fileSplit这个切换的所有数据
				IOUtils.readFully(in, contents, 0, contents.length);
				
				// 最后把读到的数据封装到value里面，value就是最后传给map方式执行的key-value的value
				value.set(contents, 0, contents.length);
				
			} finally {
				// 采用hadoop提供的工具关闭流
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	// 获取任务执行的进度
	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}
}
