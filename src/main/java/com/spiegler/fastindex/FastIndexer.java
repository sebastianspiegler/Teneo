package com.spiegler.fastindex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.commoncrawl.hadoop.io.mapred.ARCFileItemInputFormat;

public class FastIndexer extends Configured implements Tool {

	private static Log LOG = LogFactory.getLog(FastIndexer.class);
	
	private static final int EXIT_CODE_ABNORMAL = -1;
	private static final int EXIT_CODE_NORMAL = 1;
	private static final int MINARGS = 4;

	private static final String BUCKET = "aws-publicdatasets";
	private static final String PREFIX = "common-crawl/parse-output/segment/";

	public static void main(String[] args) {
		int exitCode = EXIT_CODE_ABNORMAL;
		try {
			exitCode = ToolRunner.run(new FastIndexer(), args);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		LOG.info("Exit: " + exitCode);
		return;
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(FastIndexer.class);

		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] remainingArgs = gop.getRemainingArgs();

		if (remainingArgs.length < MINARGS) {
			String message = "Wrong number of arguments. "
					+ "Provide accessKey, secretKey, s3 path file, output path.";
			LOG.error(message);
			throw new IllegalArgumentException(message);
		}
		String accessKey = remainingArgs[0];
		String secretKey = remainingArgs[1];
		String s3file = remainingArgs[2];
		String outputPath = remainingArgs[3];

		LOG.info("AWS key        : " + accessKey);
		LOG.info("AWS secret     : " + secretKey);
		LOG.info("Input file     : " + s3file);
		LOG.info("Output path    : " + outputPath);
		
		// Increase job conf limit for storing a larger number of paths (1Gb)
		conf.setLong("mapred.user.jobconf.limit", 1073741824l);

		// input/output
		conf.setInputFormat(ARCFileItemInputFormat.class);
		conf.set("mapred.input.dir", getPathsFromLocalFile(accessKey, secretKey, s3file));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		// mapper output key/value classes
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(NullWritable.class);

		// output key/value classes
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		// set mapper, no reducer
		conf.setMapperClass(FastIndexMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		
		// run job
		JobClient.runJob(conf);

		return EXIT_CODE_NORMAL;
	}
	
	/**
	 * Set input paths using local file, bootstrap large files onto EC2 instance
	 * @param accessKey
	 * @param secretKey
	 * @param inFile
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	static String getPathsFromLocalFile(String accessKey, String secretKey, String inFile){

		BufferedReader reader = null;
		String arcFile, arcPath;
		
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		int count = 0;
		try {
			reader = new BufferedReader(new FileReader(inFile));
			while (true) {
				arcFile = reader.readLine();
				if (arcFile == null) {
					break;
				}
				arcFile = PREFIX + arcFile;
				arcPath = String.format("s3://%s:%s@%s/%s", accessKey,
						secretKey, BUCKET, arcFile);
				
				if(first){
					first = false;
				}
				else{
					sb.append(StringUtils.COMMA);
				}
				sb.append(arcPath);
				count++;
				
				if (count % 1000 == 0){
					LOG.info("Added " + count + " files to StringBuffer");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(reader);
		}
		return sb.toString();
	}
}