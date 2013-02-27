package com.spiegler.index.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class CCIndexValue implements WritableComparable<CCIndexValue>{

	private LongWritable byteSize 	= new LongWritable();
	private LongWritable count		= new LongWritable();

	public CCIndexValue() { }

	public CCIndexValue(long byteSize, long count){
		this.byteSize.set(byteSize);
		this.count.set(count);
	}

	@Override
	public String toString() {
		return (new StringBuilder())
		.append('{')
		.append(this.byteSize)
		.append(',')
		.append(this.count)
		.append('}')				
		.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.byteSize.set(in.readLong());
		this.count.set(in.readLong());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.byteSize.get());
		out.writeLong(this.count.get());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
		+ ((byteSize == null) ? 0 : byteSize.hashCode());
		result = prime * result + ((count == null) ? 0 : count.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CCIndexValue other = (CCIndexValue) obj;
		if (byteSize == null) {
			if (other.byteSize != null)
				return false;
		} else if (!byteSize.equals(other.byteSize))
			return false;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		return true;
	}

	@Override
	public int compareTo(CCIndexValue other) {
		int ret = byteSize.compareTo(other.byteSize);
		if (ret == 0) {
			return count.compareTo(other.count);
		}
		return ret;
	}
	
	public LongWritable getByteSize() {
		return byteSize;
	}

	public void setByteSize(LongWritable byteSize) {
		this.byteSize = byteSize;
	}

	public LongWritable getCount() {
		return count;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}

}