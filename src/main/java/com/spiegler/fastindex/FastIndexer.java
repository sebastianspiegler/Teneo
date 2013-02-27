package com.spiegler.fastindex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.commoncrawl.hadoop.io.mapred.ARCFileItemInputFormat;


import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.spiegler.index.util.CCIndexKey;
import com.spiegler.index.util.CCIndexValue;

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

		if (remainingArgs.length != MINARGS) {
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

		// input/output
		conf.setInputFormat(ARCFileItemInputFormat.class);
		this.setInputPath(conf, accessKey, secretKey, s3file);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		// mapper output key/value classes
		conf.setMapOutputKeyClass(CCIndexKey.class);
		conf.setMapOutputValueClass(CCIndexValue.class);

		// output key/value classes
		conf.setOutputKeyClass(CCIndexKey.class);
		conf.setOutputValueClass(CCIndexValue.class);

		// set mapper, no reducer
		conf.setMapperClass(FastIndexMapper.class);
		conf.setNumReduceTasks(0);
		
		// run job
		JobClient.runJob(conf);

		return EXIT_CODE_NORMAL;
	}

	private void setInputPath(JobConf conf, String accessKey, String secretKey,
			String s3file) throws IOException, URISyntaxException {

		// get bucket/file from s3file
		URI url = new URI(s3file);
		String bucket = url.getHost();
		String file = url.getPath().substring(1);

		// get s3 object
		BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey,
				secretKey);
		AmazonS3Client s3Client = new AmazonS3Client(credentials);
		S3Object object = s3Client
				.getObject(new GetObjectRequest(bucket, file));

		// get content of s3 object, add paths to file input format
		BufferedReader reader = null;
		String arcFile, arcPath;
		try {
			reader = new BufferedReader(new InputStreamReader(
					object.getObjectContent()));
			while (true) {
				arcFile = reader.readLine();
				if (arcFile == null) {
					break;
				}
				arcFile = PREFIX + arcFile;
				arcPath = String.format("s3://%s:%s@%s/%s", accessKey,
						secretKey, BUCKET, arcFile);
				FileInputFormat.addInputPath(conf, new Path(arcPath));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}