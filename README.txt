This folder includes:


1. NASA.java

A Java program that sends filtered tweets from the Twitter Public Stream to Amazon S3 and Amazon Redshift via Amazon Kinesis Firehose.


2. scribe-1.3.3.jar

An OAuth library for Java.  Twitter uses OAuth as its authentication standard; when imported into NASA.java, this library facilitates passing credentials to connect to the Twitter Public Stream.


3. RedshiftJDBC41-1.1.9.1009.jar

The Amazon Redshift driver compatible with the JDBC 4.1 API.  Must be added to SQL Workbench/J profile to connect to Amazon Redshift.