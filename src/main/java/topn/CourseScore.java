package topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CourseScore implements WritableComparable<CourseScore> {
	private String course;
	private String name;
	private double score;

	public CourseScore() {
	}

	public CourseScore(String course, String name, double score) {
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
	public int compareTo(CourseScore cs) {
		int compareTo = this.course.compareTo(cs.getCourse());
		if (compareTo == 0) {//如果课程名相同，则判断分数之间是大、小还是等于
			double diff = cs.getScore() - this.score;
			if (diff > 0) {//从大到小，降序排列
				return 1;
			} else if (diff < 0) {
				return -1;
			} else {
				return 0;
			}
		} else {//课程名不同，直接返回课程名，按课程名分不同区
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
		return course + '\t' + name + '\t' + score;
	}
}
