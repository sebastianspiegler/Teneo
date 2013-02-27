package com.spiegler.util;

import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.record.Buffer;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.junit.Test;

import com.spiegler.util.IndexUtil;

import static org.junit.Assert.assertEquals;

public class IndexUtilTestCase {
	
	@Test
	public void testGetDomain(){
		assertEquals("google.com", 			IndexUtil.getDomain("google.com"));
		assertEquals("www.google.com", 		IndexUtil.getDomain("www.google.com"));
		assertEquals("www.google.co.uk", 	IndexUtil.getDomain("http://www.google.co.uk"));
		assertEquals("www.test.com", 		IndexUtil.getDomain("http://www.test.com/test1/test2/page.html"));
		assertEquals("parliament.uk", 		IndexUtil.getDomain("http://parliament.uk"));
		assertEquals("127.0.0.1", 			IndexUtil.getDomain("127.0.0.1"));
	}
	
	@Test
	public void testGetPublicSuffix(){
		assertEquals("com",					IndexUtil.getPublicSuffix("google.com"));
		assertEquals("com", 				IndexUtil.getPublicSuffix("www.google.com"));
		assertEquals("co.uk", 				IndexUtil.getPublicSuffix("www.google.co.uk"));
		assertEquals("co.uk", 				IndexUtil.getPublicSuffix("http://www.google.co.uk"));
		assertEquals("com", 				IndexUtil.getPublicSuffix("http://www.test.com/test1/test2/page.html"));
		assertEquals("uk", 					IndexUtil.getPublicSuffix("http://parliament.uk"));
		assertEquals("co.uk",	 			IndexUtil.getPublicSuffix("http://news.bbc.co.uk"));
		assertEquals("127.0.0.1", 			IndexUtil.getPublicSuffix("127.0.0.1"));
	}
	
	@Test
	public void testGetSecondLevelDomain(){
		assertEquals("google.com", 			IndexUtil.getSecondLevelDomain("google.com"));
		assertEquals("google.com", 			IndexUtil.getSecondLevelDomain("www.google.com"));
		assertEquals("google.co.uk", 		IndexUtil.getSecondLevelDomain("www.google.co.uk"));
		assertEquals("google.co.uk", 		IndexUtil.getSecondLevelDomain("http://www.google.co.uk"));
		assertEquals("test.com", 			IndexUtil.getSecondLevelDomain("http://www.test.com/test1/test2/page.html"));
		assertEquals("parliament.uk", 		IndexUtil.getSecondLevelDomain("http://parliament.uk"));
		assertEquals("bbc.co.uk", 			IndexUtil.getSecondLevelDomain("http://news.bbc.co.uk"));		
		assertEquals("127.0.0.1", 			IndexUtil.getSecondLevelDomain("127.0.0.1"));
	}
	
	@Test
	public void testByteSize1() throws Exception{
		ArcFileItem item	= new ArcFileItem();
		byte[] bytes = {1,2,3,4,5};
		item.setContent(new Buffer(bytes), false);
		assertEquals(5, IndexUtil.byteSize(item, "none"));
		assertEquals(5, IndexUtil.byteSize(item, "utf-8"));
		assertEquals(5, IndexUtil.byteSize(item, "iso-8859-1"));
		assertEquals(5, IndexUtil.byteSize(item, "gb18030"));
	}
	
	@Test
	(expected=UnsupportedCharsetException.class)
	public void testByteSize2() throws Exception{
		ArcFileItem item	= new ArcFileItem();
		byte[] bytes = {1,2,3,4,5};
		item.setContent(new Buffer(bytes), false);
		assertEquals(5, IndexUtil.byteSize(item, "foo"));
	}
	
	@Test
	public void testExtractCharset1(){
		List<ArcFileHeaderItem> items = new ArrayList<ArcFileHeaderItem>();
		ArcFileHeaderItem item = new ArcFileHeaderItem();
		item.setItemKey("Content-Type");item.setItemValue("text/html; charset=UTF-8");
		items.add(item);
		assertEquals("utf-8", IndexUtil.extractCharset(items));
	}
	
	@Test
	public void testExtractCharset2(){
		List<ArcFileHeaderItem> items = new ArrayList<ArcFileHeaderItem>();
		ArcFileHeaderItem item = new ArcFileHeaderItem();
		item.setItemKey("Content-Type");item.setItemValue("text/plain; charset=ISO-8859-1");
		items.add(item);
		assertEquals("iso-8859-1", IndexUtil.extractCharset(items));
	}

	@Test
	public void testExtractCharset3(){
		List<ArcFileHeaderItem> items = new ArrayList<ArcFileHeaderItem>();
		ArcFileHeaderItem item = new ArcFileHeaderItem();
		item.setItemKey("Content-Type");item.setItemValue("text/html; charset=ISO-8859-1");
		items.add(item);
		assertEquals("iso-8859-1", IndexUtil.extractCharset(items));
	}

	@Test
	public void testExtractCharset4(){
		List<ArcFileHeaderItem> items = new ArrayList<ArcFileHeaderItem>();
		ArcFileHeaderItem item = new ArcFileHeaderItem();
		item.setItemKey("Content-Type");item.setItemValue("application/x-javascript; charset=utf-8");
		items.add(item);
		assertEquals("utf-8", IndexUtil.extractCharset(items));
	}

	@Test
	public void testExtractCharset5(){
		List<ArcFileHeaderItem> items = new ArrayList<ArcFileHeaderItem>();
		ArcFileHeaderItem item = new ArcFileHeaderItem();
		item.setItemKey("foo");item.setItemValue("foo");
		items.add(item);
		assertEquals("none", IndexUtil.extractCharset(items));
	}
}
