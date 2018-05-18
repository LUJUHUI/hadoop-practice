package flow.partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class ProvincePartitioner extends Partitioner<FlowBean,NullWritable> {
	static Map<String,Integer> provinceMap = new HashMap<String,Integer>();
	static {
		provinceMap.put("134",0);
		provinceMap.put("139",1);
		provinceMap.put("137",2);
		provinceMap.put("150",3);
		provinceMap.put("159",4);
	}

	@Override
	public int getPartition(FlowBean key, NullWritable value, int numPartitions) {
		String telphone = key.getTelphone();
		String prefixThree = telphone.substring(0,3);
		if(!provinceMap.containsKey(prefixThree)){
			return 5;
		}else{
			return provinceMap.get(prefixThree);
		}
	}
}
