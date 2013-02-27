package com.spiegler.fastindex;

import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.record.Buffer;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.junit.Before;
import org.junit.Test;

public class FastIndexerTestCase {

	private ArcFileItem item1;

	private MapDriver<Text, ArcFileItem, Text, NullWritable> mapDriver;
	
	@Before
	public void setUp(){
		item1 = createItem("file1", "text/html", "http://www.google.com/search", "abc", "charset=UTF-8");
		
		Mapper<Text, ArcFileItem, Text, NullWritable> mapper			= new FastIndexMapper();
		mapDriver 	 = new MapDriver<Text, ArcFileItem, Text, NullWritable>();
		mapDriver.setMapper(mapper);
	}

	@Test
	public void testMapper(){
		JobConf conf = new JobConf();
		mapDriver.withConfiguration(conf)
		.withInput(new Text(), item1)
		.withOutput(new Text("com\tgoogle.com\ttext/html\tutf-8\tfile1\t3"), NullWritable.get())
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
