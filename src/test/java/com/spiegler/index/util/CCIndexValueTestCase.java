package com.spiegler.index.util;

import static org.junit.Assert.*;

import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import com.spiegler.index.util.CCIndexValue;

public class CCIndexValueTestCase {

	private CCIndexValue value1;
	private CCIndexValue value2;
	private CCIndexValue value3;
	private CCIndexValue value4;
	
	@Before
	public void setUp() throws Exception {
		this.value1 = new CCIndexValue(1,1);
		this.value2 = new CCIndexValue(1,1);
		this.value3 = new CCIndexValue(2,1);
		this.value4 = new CCIndexValue(1,2);
	}

	@Test
	public void testToString() {
		assertEquals("{1,1}",this.value1.toString());
		assertEquals("{2,1}",this.value3.toString());
		assertEquals("{1,2}",this.value4.toString());
	}

	@Test
	public void testEqualsObject() {
		assertEquals( true, this.value1.equals(this.value2));
		assertEquals( true, this.value2.equals(this.value1));
		
		assertEquals(false, this.value1.equals(this.value3));
		assertEquals(false, this.value1.equals(this.value4));
	}

	@Test
	public void testCompareTo() {
		assertEquals( 0, this.value1.compareTo(this.value2));
		assertEquals( 0, this.value2.compareTo(this.value1));
		
		assertEquals(-1, this.value1.compareTo(this.value3));
		assertEquals(-1, this.value1.compareTo(this.value4));

		assertEquals( 0, this.value1.compareTo(this.value2));
		assertEquals( 0, this.value2.compareTo(this.value1));
		
		assertEquals(-1, this.value1.compareTo(this.value3));
		assertEquals(-1, this.value1.compareTo(this.value4));

		assertEquals( 1, this.value3.compareTo(this.value1));
		assertEquals( 1, this.value4.compareTo(this.value1));
	}

	@Test
	public void testGetByteSize() {
		assertEquals(new LongWritable(1), this.value1.getByteSize());
		assertEquals(new LongWritable(2), this.value3.getByteSize());
	}

	@Test
	public void testSetByteSize() {
		CCIndexValue value = new CCIndexValue(1, 1);
		assertEquals(new LongWritable(1), value.getByteSize());
		value.setByteSize(new LongWritable(2));
		assertEquals(new LongWritable(2), value.getByteSize());
	}

	@Test
	public void testGetCount() {
		assertEquals(new LongWritable(1), this.value1.getCount());
		assertEquals(new LongWritable(2), this.value4.getCount());
	}

	@Test
	public void testSetCount() {
		CCIndexValue value = new CCIndexValue(1, 1);
		assertEquals(new LongWritable(1), value.getCount());
		value.setCount(new LongWritable(2));
		assertEquals(new LongWritable(2), value.getCount());
	}

}
