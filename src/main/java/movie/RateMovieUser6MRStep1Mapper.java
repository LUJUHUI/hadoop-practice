package movie;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RateMovieUser6MRStep1Mapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
}
