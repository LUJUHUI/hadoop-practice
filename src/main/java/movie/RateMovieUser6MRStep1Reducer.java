package movie;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RateMovieUser6MRStep1Reducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
}
