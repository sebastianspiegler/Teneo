# Hive queries for aggregate statistics of the common crawl corpus

This is a selection of the Hive queries used for aggregating the statistics of the 
Common Crawl corpus 2012.

## Load and describe table

```
hive> CREATE EXTERNAL TABLE crawl (psuffix STRING, domain STRING, mtype STRING, charset STRING, fname STRING, bytes BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 's3://[bucket]/';
```

```
hive> DESCRIBE crawl;
OK
psuffix	string	
domain	string	
mtype	string	
charset	string	
fname	string	
bytes	bigint
```

## Distinct domains per public suffix

```
hive> SELECT psuffix, COUNT(DISTINCT(domain)) AS total FROM crawl GROUP BY psuffix SORT BY total DESC;
```

## Documents and bytes per public suffix

```
hive> SELECT psuffix, COUNT(domain) AS total FROM crawl GROUP BY psuffix SORT BY total DESC;
hive> SELECT psuffix, SUM(bytes) AS total FROM crawl GROUP BY psuffix SORT BY total DESC;
```

## Media types per public suffix
```
hive> SELECT psuffix, mtype, COUNT(mtype) as total FROM crawl GROUP BY psuffix, mtype SORT BY total DESC;
hive> SELECT psuffix, mtype, SUM(bytes) as total FROM crawl GROUP BY psuffix, mtype SORT BY total DESC;
```

## Top domains per public suffix
```
hive> SELECT psuffix, domain, COUNT(domain) as total FROM crawl GROUP BY psuffix, domain SORT BY total DESC;
hive> SELECT psuffix, domain, SUM(bytes) as total FROM crawl GROUP BY psuffix, domain SORT BY total DESC;
```

## Encodings
```
hive> SELECT charset, SUM(bytes) AS total FROM crawl GROUP BY charset SORT BY total DESC;
hive> SELECT charset, COUNT(charset) AS total FROM crawl GROUP BY charset SORT BY total DESC;
hive> SELECT psuffix, charset, COUNT(charset) AS total FROM crawl GROUP BY psuffix, charset SORT BY total DESC;
```

## Media types
```
hive> SELECT psuffix, mtype, COUNT(mtype) as total FROM crawl GROUP BY psuffix, mtype SORT BY total DESC;
hive> SELECT psuffix, mtype, SUM(bytes) as total FROM crawl GROUP BY psuffix, mtype SORT BY total DESC;
```