package com.spiegler.index.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CCIndexKey implements WritableComparable<CCIndexKey> {

	private Text psuffix = new Text();
	private Text domain  = new Text();
	private Text mtype 	 = new Text();
	private Text charset = new Text();
	private Text fname 	 = new Text();

	public CCIndexKey() { }
	
	public CCIndexKey(
			String psuffix,
			String domain,
			String mtype,
			String charset,
			String fname){
		this.psuffix.set(psuffix);
		this.domain.set(domain);
		this.mtype.set(mtype);
		this.charset.set(charset);
		this.fname.set(fname);
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(this.psuffix)
				.append(',')
				.append(this.domain)
				.append(',')
				.append(this.mtype)
				.append(',')
				.append(this.charset)
				.append(',')				
				.append(this.fname)
				.append('}')
				.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.psuffix.set(in.readUTF());
		this.domain.set(in.readUTF());
		this.mtype.set(in.readUTF());
		this.charset.set(in.readUTF());
		this.fname.set(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.psuffix.toString());
		out.writeUTF(this.domain.toString());
		out.writeUTF(this.mtype.toString());
		out.writeUTF(this.charset.toString());
		out.writeUTF(this.fname.toString());
	}

	@Override
	public int compareTo(CCIndexKey o) {
		int result = psuffix.compareTo(o.psuffix);
		if(result == 0) {
			result = domain.compareTo(o.domain);
			if(result == 0) {
				result = mtype.compareTo(o.mtype);
				if(result == 0) {
					result = charset.compareTo(o.charset);
					if(result == 0) {
						result = fname.compareTo(o.fname);
					}
				}
			}
		}
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((charset == null) ? 0 : charset.hashCode());
		result = prime * result + ((domain == null) ? 0 : domain.hashCode());
		result = prime * result + ((fname == null) ? 0 : fname.hashCode());
		result = prime * result + ((mtype == null) ? 0 : mtype.hashCode());
		result = prime * result + ((psuffix == null) ? 0 : psuffix.hashCode());
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
		CCIndexKey other = (CCIndexKey) obj;
		if (charset == null) {
			if (other.charset != null)
				return false;
		} else if (!charset.equals(other.charset))
			return false;
		if (domain == null) {
			if (other.domain != null)
				return false;
		} else if (!domain.equals(other.domain))
			return false;
		if (fname == null) {
			if (other.fname != null)
				return false;
		} else if (!fname.equals(other.fname))
			return false;
		if (mtype == null) {
			if (other.mtype != null)
				return false;
		} else if (!mtype.equals(other.mtype))
			return false;
		if (psuffix == null) {
			if (other.psuffix != null)
				return false;
		} else if (!psuffix.equals(other.psuffix))
			return false;
		return true;
	}

	public Text getFname() {
		return fname;
	}

	public void setFname(Text fname) {
		this.fname = fname;
	}

	public Text getMtype() {
		return mtype;
	}

	public void setMtype(Text mtype) {
		this.mtype = mtype;
	}

	public Text getDomain() {
		return domain;
	}

	public void setDomain(Text domain) {
		this.domain = domain;
	}

	public Text getCharset() {
		return charset;
	}

	public void setCharset(Text charset) {
		this.charset = charset;
	}

	public Text getPsuffix() {
		return psuffix;
	}

	public void setPsuffix(Text psuffix) {
		this.psuffix = psuffix;
	}
}