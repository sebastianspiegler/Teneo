package com.spiegler.fastindex;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;


import org.apache.commons.io.IOUtils;
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

	private File inFile;
	private ArcFileItem item1;

	private MapDriver<Text, ArcFileItem, Text, NullWritable> mapDriver;
	
	@Before
	public void setUp() throws IOException{
		item1 = createItem("file1", "text/html", "http://www.google.com/search", "abc", "charset=UTF-8");
		
		Mapper<Text, ArcFileItem, Text, NullWritable> mapper			= new FastIndexMapper();
		mapDriver 	 = new MapDriver<Text, ArcFileItem, Text, NullWritable>();
		mapDriver.setMapper(mapper);
		
		// dictionary File
		BufferedWriter out = null;
		inFile = File.createTempFile(String.valueOf(System.currentTimeMillis() + 1), ".txt");
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(inFile),"UTF8"));
		out.write("a\n");
		out.write("b\n");
		out.write("c\n");
		IOUtils.closeQuietly(out);
	}

	@Test
	public void testMapper(){
		JobConf conf = new JobConf();
		mapDriver.withConfiguration(conf)
		.withInput(new Text(), item1)
		.withOutput(new Text("com\tgoogle.com\ttext/html\tutf-8\tfile1\t3"), NullWritable.get())
		.runTest();
	}
	
	@Test
	public void testGetPathsFromLocalFile(){
		String accessKey = "accessKey";
		String secretKey = "secretKey";
		String expected = "s3://accessKey:secretKey@aws-publicdatasets/common-crawl/parse-output/segment/a," +
				"s3://accessKey:secretKey@aws-publicdatasets/common-crawl/parse-output/segment/b," +
				"s3://accessKey:secretKey@aws-publicdatasets/common-crawl/parse-output/segment/c";
		String result = FastIndexer.getPathsFromLocalFile(accessKey, secretKey, inFile.getAbsolutePath());
		assertEquals(expected, result);
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
