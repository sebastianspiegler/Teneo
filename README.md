# Statistics of the Common Crawl Corpus 2012

Link to [Common Crawl Foundation](http://commoncrawl.org/)

Link to [Actual paper](https://docs.google.com/file/d/1_9698uglerxB9nAglvaHkEgU-iZNm1TvVGuCW7245-WGvZq47teNpb_uL5N9/edit)

Raw index on AWS S3: ```s3://aws-publicdatasets/common-crawl/index201'''

## MapReduce code

The job run for creating the raw index is [com.spiegler.fastindex.FastIndexer.java](https://github.com/sebastianspiegler/Teneo/blob/master/src/main/java/com/spiegler/fastindex/FastIndexer.java).
It is a 'map only' job which creates a single line entry for each website found in the CC corpus. 

Each line contains the public suffix, domain, media type, charset, ARC file name and byte size of a specific website, all tab separated.

Build the job jar by running
```
$ ant dist
```
to create  ```dist/lib/Teneo-########.jar'''.

## Run job on AWS
The job was run on 35 subsets of 25,000 ARC files of the 2012 corpus. Results were later merged into fewer files.

A job on a subset was invoked by
```
elastic-mapreduce  --create --credentials credentials.json \
 --jar s3://[bucket]/Teneo-########.jar \
 --main-class com.spiegler.fastindex.FastIndexer \
 --args "[AccessKey],[SecretKey],[ARC file input list],[Output bucket]" \
 --instance-group master --instance-type m1.xlarge --instance-count 1 --bid-price [$$$] \
 --instance-group core   --instance-type m1.xlarge --instance-count 5 --bid-price [$$$] \
 --bootstrap-action s3://elasticmapreduce/bootstrap-actions/install-ganglia \
 --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive \
 --bootstrap-action s3://ccindex2012/jars/bootstrap_splits.sh \
 --key-pair [YourKey] \
 --log-uri s3n://[bucket] \
 --enable-debugging
```
where the arguments for the job where the access key, secret key, a file containing the ARC file input list (bootstrapped onto instances) and an output S3 bucket.

## Hive code

For the actual aggregation Hive has been used. Some examples are provided (here)[https://github.com/sebastianspiegler/Teneo/blob/master/hive/README.md].






