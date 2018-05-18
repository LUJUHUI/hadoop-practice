package flow.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FlowBeanSort implements WritableComparable<FlowBeanSort> {
	private String telphoneNum;
	private long upFlow;
	private long downFlow;
	private long sumFlow;

	public FlowBeanSort() {
	}

	public void set(long upFlow, long downFlow) {
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

	@Override
	public String toString() {
		return "上行流量:" + upFlow + "\t" + "下行流量:" + downFlow + "\t" + "总流量:" + sumFlow;
	}

	/*排序，按照总流量从大到小，倒序排列*/
	@Override
	public int compareTo(FlowBeanSort fb) {
		return (int) (fb.getSumFlow() - this.sumFlow);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
}
