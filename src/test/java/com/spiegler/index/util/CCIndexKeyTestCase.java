package com.spiegler.index.util;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.spiegler.index.util.CCIndexKey;

public class CCIndexKeyTestCase {

	private CCIndexKey key1;
	private CCIndexKey key2;
	private CCIndexKey key3;
	private CCIndexKey key4;
	private CCIndexKey key5;
	private CCIndexKey key6;
	private CCIndexKey key7;
	
	@Before
	public void setUp() throws Exception {
		this.key1 = new CCIndexKey("a", "a", "a", "a", "a");
		this.key2 = new CCIndexKey("a", "a", "a", "a", "a");
		this.key3 = new CCIndexKey("b", "a", "a", "a", "a");	// psuffix = b
		this.key4 = new CCIndexKey("a", "b", "a", "a", "a");	// domain  = b
		this.key5 = new CCIndexKey("a", "a", "b", "a", "a");	// mtype   = b
		this.key6 = new CCIndexKey("a", "a", "a", "b", "a");	// charset = b
		this.key7 = new CCIndexKey("a", "a", "a", "a", "b");	// fname   = b
	}

	@Test
	public void testCompareTo() {
		assertEquals( 0, this.key1.compareTo(this.key2));
		assertEquals( 0, this.key2.compareTo(this.key1));
		
		assertEquals(-1, this.key1.compareTo(this.key3));
		assertEquals( 1, this.key3.compareTo(this.key1));
		
		assertEquals(-1, this.key1.compareTo(this.key4));
		assertEquals(-1, this.key1.compareTo(this.key5));
		assertEquals(-1, this.key1.compareTo(this.key6));
		assertEquals(-1, this.key1.compareTo(this.key7));

		assertEquals( 1, this.key4.compareTo(this.key1));
		assertEquals( 1, this.key5.compareTo(this.key1));
		assertEquals( 1, this.key6.compareTo(this.key1));
		assertEquals( 1, this.key7.compareTo(this.key1));
	}

	@Test
	public void testEqualsObject() {
		assertEquals( true, this.key1.equals(this.key2));
		assertEquals( true, this.key2.equals(this.key1));

		assertEquals(false, this.key1.equals(this.key3));
		assertEquals(false, this.key1.equals(this.key4));
		assertEquals(false, this.key1.equals(this.key5));
		assertEquals(false, this.key1.equals(this.key6));
		assertEquals(false, this.key1.equals(this.key7));

		assertEquals(false, this.key3.equals(this.key1));
		assertEquals(false, this.key4.equals(this.key1));
		assertEquals(false, this.key5.equals(this.key1));
		assertEquals(false, this.key6.equals(this.key1));
		assertEquals(false, this.key7.equals(this.key1));
	}

	@Test
	public void testGetFname() {
		assertEquals(new Text("a"), this.key1.getFname());
		assertEquals(new Text("b"), this.key7.getFname());
	}

	@Test
	public void testSetFname() {
		CCIndexKey key = new CCIndexKey("a", "a", "a", "a", "a");
		assertEquals(new Text("a"), key.getFname());
		key.setFname(new Text("b"));
		assertEquals(new Text("b"), key.getFname());
	}

	@Test
	public void testGetMtype() {
		assertEquals(new Text("a"), this.key1.getMtype());
		assertEquals(new Text("b"), this.key5.getMtype());
	}

	@Test
	public void testSetMtype() {
		CCIndexKey key = new CCIndexKey("a", "a", "a", "a", "a");
		assertEquals(new Text("a"), key.getMtype());
		key.setMtype(new Text("b"));
		assertEquals(new Text("b"), key.getMtype());
	}

	@Test
	public void testGetDomain() {
		assertEquals(new Text("a"), this.key1.getDomain());
		assertEquals(new Text("b"), this.key4.getDomain());
	}

	@Test
	public void testSetDomain() {
		CCIndexKey key = new CCIndexKey("a", "a", "a", "a", "a");
		assertEquals(new Text("a"), key.getDomain());
		key.setDomain(new Text("b"));
		assertEquals(new Text("b"), key.getDomain());
	}

	@Test
	public void testGetCharset() {
		assertEquals(new Text("a"), this.key1.getCharset());
		assertEquals(new Text("b"), this.key6.getCharset());
	}

	@Test
	public void testSetCharset() {
		CCIndexKey key = new CCIndexKey("a", "a", "a", "a", "a");
		assertEquals(new Text("a"), key.getCharset());
		key.setCharset(new Text("b"));
		assertEquals(new Text("b"), key.getCharset());
	}

	@Test
	public void testGetPsuffix() {
		assertEquals(new Text("a"), this.key1.getPsuffix());
		assertEquals(new Text("b"), this.key3.getPsuffix());
	}

	@Test
	public void testSetPsuffix() {
		CCIndexKey key = new CCIndexKey("a", "a", "a", "a", "a");
		assertEquals(new Text("a"), key.getPsuffix());
		key.setPsuffix(new Text("b"));
		assertEquals(new Text("b"), key.getPsuffix());
	}
}
