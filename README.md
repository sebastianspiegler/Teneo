# Statistics of the Common Crawl Corpus 2012

## References
* [Common Crawl Foundation](http://commoncrawl.org/)
* [The actual paper](https://docs.google.com/file/d/1_9698uglerxB9nAglvaHkEgU-iZNm1TvVGuCW7245-WGvZq47teNpb_uL5N9/edit)
* Raw index on AWS S3: `s3://aws-publicdatasets/common-crawl/index201`

## MapReduce code

The job run for creating the raw index is [com.spiegler.fastindex.FastIndexer.java](https://github.com/sebastianspiegler/Teneo/blob/master/src/main/java/com/spiegler/fastindex/FastIndexer.java).
It is a 'map only' job which outputs a single line entry for each website found in the CC corpus. 

Each line contains the public suffix, domain, media type, charset, ARC file name and byte size of a specific website, all tab separated.

Build the job jar by running:
```
$ ant dist
```
to create  `dist/lib/Teneo-########.jar`.

## Run job on AWS
The job was run on 35 subsets of 25,000 ARC files of the 2012 corpus. Results were later merged into fewer files.

A job on a subset was invoked by
```
elastic-mapreduce  --create --credentials credentials.json \
 --jar s3://[bucket]/Teneo-########.jar \
 --main-class com.spiegler.fastindex.FastIndexer \
 --args "[AccessKey],[SecretKey],/home/hadoop/splits/split_1,s3://[bucket]/output/split_1" \
 --instance-group master --instance-type m1.xlarge --instance-count 1 --bid-price [$$$] \
 --instance-group core   --instance-type m1.xlarge --instance-count 5 --bid-price [$$$] \
 --bootstrap-action s3://elasticmapreduce/bootstrap-actions/install-ganglia \
 --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive \
 --bootstrap-action s3://[bucket]/bootstrap_splits.sh \
 --key-pair [YourKey] \
 --log-uri s3n://[bucket] \
 --enable-debugging
```
where the arguments for the job are the access key, secret key, a file containing the ARC file input list (bootstrapped onto instances) and an output S3 bucket.

The bootstrapping script `bootstrap_splits.sh` for copying split files onto the instances
```
#!/bin/bash
set -e
mkdir -p /home/hadoop/splits/
hadoop fs -copyToLocal s3://[bucket]/splits/* /home/hadoop/splits/
```

An example for a split, e.g. `split_1`
```
1346823845675/1346864466526_10.arc.gz
1346823845675/1346864469604_0.arc.gz
1346823845675/1346864469638_1.arc.gz
1346823845675/1346864471290_4.arc.gz
1346823845675/1346864477152_29.arc.gz
...
```

## Hive code

For the actual aggregation Hive was used. Some examples are provided [here](https://github.com/sebastianspiegler/Teneo/blob/master/hive/README.md).






