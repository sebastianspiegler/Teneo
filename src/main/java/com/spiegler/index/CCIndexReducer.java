package com.spiegler.index;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.spiegler.index.util.CCIndexKey;
import com.spiegler.index.util.CCIndexValue;

/**
 * Reducer

 * @author spiegler
 *
 */
public class CCIndexReducer extends MapReduceBase implements Reducer<CCIndexKey, CCIndexValue, CCIndexKey, CCIndexValue> { 
	
	@Override
	public void reduce(CCIndexKey key, Iterator<CCIndexValue> values, 
			OutputCollector<CCIndexKey, CCIndexValue> output, 
			Reporter reporter) throws IOException {
		long byteSum 	= 0;
		long countSum 	= 0;
		while (values.hasNext()){
			CCIndexValue value = values.next();
			byteSum 	+= value.getByteSize().get();
			countSum 	+= value.getCount().get();
		}
		output.collect(key, new CCIndexValue(byteSum, countSum));
		reporter.incrCounter("CCIndexReducer", "files", countSum);
		reporter.incrCounter("CCIndexReducer", "bytes", byteSum);
	}
}