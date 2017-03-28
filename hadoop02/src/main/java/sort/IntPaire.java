package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class IntPaire implements WritableComparable<IntPaire> {
	
	private String firstKey;
	private int secondKey;

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		firstKey = in.readUTF();
		secondKey = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(firstKey);
		out.writeInt(secondKey);
	}

	@Override
	public int compareTo(IntPaire o) {
		// TODO Auto-generated method stub
		return o.getFirstKey().compareTo(this.firstKey);
	}

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public int getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(int secondKey) {
		this.secondKey = secondKey;
	}
	
}
