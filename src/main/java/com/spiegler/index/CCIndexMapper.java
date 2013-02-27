package com.spiegler.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.shared.ArcFileItem;

import com.spiegler.index.util.CCIndexKey;
import com.spiegler.index.util.CCIndexValue;
import com.spiegler.util.IndexUtil;

/**
 * Mapper
 * 
 * @author spiegler
 *
 */
public class CCIndexMapper extends MapReduceBase implements Mapper<Text, ArcFileItem, CCIndexKey, CCIndexValue>{
	
	@Override
	public void map(Text key, ArcFileItem value, OutputCollector<CCIndexKey, CCIndexValue> output, Reporter reporter) throws IOException {
		try{
			String fname 	= value.getArcFileName();
			String mtype 	= value.getMimeType();
			String uri   	= value.getUri();
			String domain	= IndexUtil.getSecondLevelDomain(uri);
			String psuffix	= IndexUtil.getPublicSuffix(domain);
			String charset 	= IndexUtil.extractCharset(value.getHeaderItems());
			Long byteSize 	= IndexUtil.byteSize(value, charset);
			
			CCIndexKey k 	= new CCIndexKey(psuffix, domain, mtype, charset, fname);
			CCIndexValue v 	= new CCIndexValue(byteSize, 1);
			output.collect(k, v);
			
			reporter.incrCounter("CCIndexMapper", "files", 1);
			reporter.incrCounter("CCIndexMapper", "bytes", byteSize);
		}
		catch (Exception e) {
			reporter.incrCounter("CCIndexMapper.exception", 
					e.getClass().getSimpleName(),
					1);
		}
	}
}