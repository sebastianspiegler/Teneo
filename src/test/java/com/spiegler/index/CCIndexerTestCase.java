package com.spiegler.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.junit.Before;
import org.junit.Test;

import com.spiegler.index.CCIndexMapper;
import com.spiegler.index.CCIndexReducer;
import com.spiegler.index.util.CCIndexKey;
import com.spiegler.index.util.CCIndexValue;

public class CCIndexerTestCase {

	private ArcFileItem item1;
	private ArcFileItem item2;
	private ArcFileItem item3;
	private ArcFileItem item4;

	private MapDriver<Text, ArcFileItem, CCIndexKey, CCIndexValue> mapDriver;
	private ReduceDriver<CCIndexKey, CCIndexValue, CCIndexKey, CCIndexValue> reduceDriver;
	private MapReduceDriver<Text, ArcFileItem, CCIndexKey, CCIndexValue, CCIndexKey, CCIndexValue> mrDriver;
	
	@Before
	public void setUp(){
		item1 = createItem("file1", "text/html", "http://www.google.com/search", "abc", "charset=UTF-8");
		item2 = createItem("file1", "text/html", "http://www.google.com/images", "abcd", "charset=UTF-8");
		item3 = createItem("file2", "text/html", "http://www.google.com/images", "abc", "charset=UTF-8");
		item4 = createItem("file2", "text/html", "http://www.google.com/images", "abcd", "charset=foo");
		
		Mapper<Text, ArcFileItem, CCIndexKey, CCIndexValue> mapper			= new CCIndexMapper();
		Reducer<CCIndexKey, CCIndexValue, CCIndexKey, CCIndexValue> reducer = new CCIndexReducer();
		mapDriver 	 = new MapDriver<Text, ArcFileItem, CCIndexKey, CCIndexValue>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<CCIndexKey, CCIndexValue, CCIndexKey, CCIndexValue>();
		reduceDriver.setReducer(reducer);
		mrDriver = new MapReduceDriver<Text, ArcFileItem, CCIndexKey, CCIndexValue, CCIndexKey, CCIndexValue>(mapper, reducer);
		mrDriver.setMapper(mapper);
		mrDriver.setReducer(reducer);
	}

	@Test
	public void testMapper(){
		JobConf conf = new JobConf();
		mapDriver.withConfiguration(conf)
		.withInput(new Text(), item1)
		.withOutput(new CCIndexKey("com", "google.com","text/html", "utf-8","file1"), new CCIndexValue(3,1))
		.runTest();
	}
	
	@Test
	public void testReducer(){
		CCIndexKey key = new CCIndexKey("com", "google.com","text/html", "utf-8","file1");
		List<CCIndexValue> list = new ArrayList<CCIndexValue>();
		list.add(new CCIndexValue(1,4));
		list.add(new CCIndexValue(2,5));
		list.add(new CCIndexValue(3,6));
		JobConf conf = new JobConf();
		reduceDriver.withConfiguration(conf)
		.withInput(key, list)
		.withOutput(key, new CCIndexValue(6,15))
		.runTest();
	}
	
	@Test
	public void testMapReduceJob(){
		JobConf conf = new JobConf();
		mrDriver.withConfiguration(conf)
		.withInput(new Text(), item1)
		.withInput(new Text(), item2)
		.withInput(new Text(), item3)
		.withInput(new Text(), item4)
		.withOutput(new CCIndexKey("com", "google.com","text/html", "utf-8","file1"), new CCIndexValue(7,2))
		.withOutput(new CCIndexKey("com", "google.com","text/html", "utf-8","file2"), new CCIndexValue(3,1))
		.withCounter("CCIndexMapper.exception", "UnsupportedCharsetException", 1)
		.runTest();
	}
	
	private static ArcFileItem createItem(
			String fname,
			String mtype,
			String uri,
			String content,
			String charsetStr){
		
		// general
		ArcFileItem item = new ArcFileItem();
		item.setArcFileName(fname);
		item.setMimeType(mtype);
		item.setUri(uri);
		item.setContent(new Buffer(content.getBytes()), false);
		
		// header
		ArrayList<ArcFileHeaderItem> headerItems = new ArrayList<ArcFileHeaderItem>();
		ArcFileHeaderItem header = new ArcFileHeaderItem();
		header.setItemKey("Content-Type");
		header.setItemValue(charsetStr);
		headerItems.add(header);
		item.setHeaderItems(headerItems);
		return item;
	}

}
