package com.spiegler.fastindex;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.shared.ArcFileItem;

import com.spiegler.util.IndexUtil;

/**
 * Mapper
 * 
 * @author spiegler
 *
 */
public class FastIndexMapper extends MapReduceBase implements Mapper<Text, ArcFileItem, Text, NullWritable>{
	
	private static final String FORMAT = "%s\t%s\t%s\t%s\t%s\t%d\t%d";
	
	@Override
	public void map(Text key, ArcFileItem value, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
		try{
			String fname 	= value.getArcFileName();
			String mtype 	= value.getMimeType();
			String uri   	= value.getUri();
			String domain	= IndexUtil.getSecondLevelDomain(uri);
			String psuffix	= IndexUtil.getPublicSuffix(domain);
			String charset 	= IndexUtil.extractCharset(value.getHeaderItems());
			Long byteSize 	= IndexUtil.byteSize(value, charset);
			
			String k = String.format(FORMAT, psuffix, domain, mtype, charset, fname, byteSize, 1);
			output.collect(new Text(k), NullWritable.get());
			
			reporter.incrCounter("FastIndexMapper", "files", 1);
			reporter.incrCounter("FastIndexMapper", "bytes", byteSize);
		}
		catch (Exception e) {
			System.out.println(e);
			reporter.incrCounter("FastIndexMapper.exception", e.getClass().getSimpleName(), 1);
		}
	}
}