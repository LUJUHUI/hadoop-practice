package topn.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CourseBean implements WritableComparable<CourseBean> {
	private String course;
	private String name;
	private double score;

	public CourseBean() {
	}

	public CourseBean(String course, String name, double score) {
		this.course = course;
		this.name = name;
		this.score = score;
	}

	public String getCourse() {
		return course;
	}

	public void setCourse(String course) {
		this.course = course;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compareTo(CourseBean cb) {
		int compareTo = this.getCourse().compareTo(cb.getCourse());
		if (compareTo == 0) {
			double diff = cb.getScore() - this.score;
			if (diff > 0) {
				return 1;
			} else if (diff < 0) {
				return -1;
			} else {
				return 0;
			}
		} else {
			return compareTo;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(course);
		out.writeUTF(name);
		out.writeDouble(score);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.course = in.readUTF();
		this.name = in.readUTF();
		this.score = in.readDouble();
	}

	@Override
	public String toString() {
		return "科目:" + course + '\t' + "姓名:" + name + '\t' + "分数:" + score ;
	}
}
