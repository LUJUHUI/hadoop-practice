package flow.partitioner;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
	private String telphone;
	private long upFlow;
	private long downFlow;
	private long sumFlow;

	public FlowBean() {
	}

	public void set(String telphone, long upFlow, long downFlow) {
		this.telphone = telphone;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	public String getTelphone() {
		return telphone;
	}

	public void setTelphone(String telphone) {
		this.telphone = telphone;
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
		/*return telphone + "\t" +  upFlow + "\t" +  downFlow + "\t" +  sumFlow;*/
		return "手机号:" + telphone + "\t" + "上行流量:" + upFlow + "\t" + "下行流量:" + downFlow + "\t" + "总流量:" + sumFlow;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telphone);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.telphone = in.readUTF();
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	@Override
	public int compareTo(FlowBean fb) {
		long result = fb.sumFlow - this.sumFlow;
		if (result > 0) {
			return 1;
		} else if (result < 0) {
			return -1;
		} else {
			return 0;
		}
	}
}

