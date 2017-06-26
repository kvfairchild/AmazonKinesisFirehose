package twitterFirehose;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.scribe.builder.*;
import org.scribe.builder.api.*;
import org.scribe.model.*;
import org.scribe.oauth.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CopyCommand;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.RedshiftDestinationConfiguration;
import com.amazonaws.services.kinesisfirehose.model.S3DestinationConfiguration;

public class NASA {
	
	private static final String firehoseName = "Delivery stream name goes here";
    private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/filter.json";
	private static final String key = "Twitter Key goes here";
	private static final String secret = "Twitter Secret goes here";
	private static final String token = "Twitter Token goes here";
	private static final String tokenSecret = "Twitter Token Secret goes here";
	private static final String searchTerm = "Search term goes here";
    
    public static void main(String[] args){
    	
    	// Set up AWS Credentials and Kinesis Firehose client
    	
    	 AWSCredentials credentials = null;
         try {
             credentials = new ProfileCredentialsProvider("default").getCredentials();
         } catch (Exception e) {
             throw new AmazonClientException(
                     "Cannot load the credentials from the credential profiles file. " +
                     "Please make sure that your credentials file is at the correct " +
                     "location and in valid format.",
                     e);
         }

         AmazonKinesisFirehose firehose = new AmazonKinesisFirehoseClient(credentials);
    	
         try {
         	
        	// Create new Kinesis Firehose deliveryStream
        	 
        	System.out.println("Creating deliveryStream '" + firehoseName + "'\n");        	
        	CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
        	createDeliveryStreamRequest.setDeliveryStreamName(firehoseName);
        	
        	// Structure existing S3 bucket for data delivery

        	S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration();
        	
        	String s3ARN = "S3 ARN goes here";
			s3DestinationConfiguration.setBucketARN(s3ARN);
			
			String s3Prefix = "S3 Prefix goes here";
			s3DestinationConfiguration.setPrefix(s3Prefix);
        	
        	String iamRoleARN = "IAM Role ARN goes here";
        	s3DestinationConfiguration.setRoleARN(iamRoleARN);
			
        	Integer destinationSizeInMBs = 1;
        	Integer destinationIntervalInSeconds = 60;

        	BufferingHints bufferingHints = null;
        	if (destinationSizeInMBs != null || destinationIntervalInSeconds != null) {
        	    bufferingHints = new BufferingHints();
        	    bufferingHints.setSizeInMBs(destinationSizeInMBs);
        	    bufferingHints.setIntervalInSeconds(destinationIntervalInSeconds);
        	}
        	s3DestinationConfiguration.setBufferingHints(bufferingHints);

        	// Structure existing Redshift database to receive copy of S3 contents
        	
        	CopyCommand copyCommand = new CopyCommand();
        	copyCommand.withDataTableName("Redshift table name goes here");
        
        	RedshiftDestinationConfiguration redshiftDestinationConfiguration = new RedshiftDestinationConfiguration();

        	redshiftDestinationConfiguration.withClusterJDBCURL("Redshift Cluster JDBC URL goes here")
        	    .withRoleARN(iamRoleARN)
        	    .withUsername("Redshift username goes here")
        	    .withPassword("Redshift password goes here")
        	    .withCopyCommand(copyCommand)
        	    .withS3Configuration(s3DestinationConfiguration)
        	    .withCopyCommand(copyCommand);

        	createDeliveryStreamRequest.setRedshiftDestinationConfiguration(redshiftDestinationConfiguration);
        	firehose.createDeliveryStream(createDeliveryStreamRequest);
        	
    		// Give deliveryStream time to create    
        	System.out.println("Sleeping for 30 seconds to ensure finalized deliveryStream activation...\n");
        	Thread.sleep(30000);
        	
            DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
            describeDeliveryStreamRequest.withDeliveryStreamName(firehoseName);
            DescribeDeliveryStreamResult describeDeliveryStreamResponse =
                    firehose.describeDeliveryStream(describeDeliveryStreamRequest);
            String status = describeDeliveryStreamResponse.getDeliveryStreamDescription().getDeliveryStreamStatus();

        	if (status.equals("CREATING")){
            	System.out.println("DeliveryStream not yet active; sleeping for an additional 10 seconds...\n");    
                Thread.sleep(10000);
        	}
        	
            System.out.println("Firehose '" + firehoseName + "' created!\n");
            
         } catch (AmazonClientException | InterruptedException ace) {
             
             System.out.println("Error Message: " + ace.getMessage() + "\n");
         }
        	                	         
        try{
            System.out.println("Starting Twitter public stream consumer for search term '" + searchTerm + "'\n");

            // Pass Twitter consumer key and secret to OAuth
            OAuthService service = new ServiceBuilder()
                    .provider(TwitterApi.class)
                    .apiKey(key)
                    .apiSecret(secret)
                    .build();

            // Set Twitter access token and token secret
            Token accessToken = new Token(token, tokenSecret);

            // Generate the request to search public stream
            System.out.println("Connecting to Twitter Public Stream...\n");
            OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
            request.addHeader("version", "HTTP/1.1");
            request.addHeader("host", "stream.twitter.com");
            request.setConnectionKeepAlive(true);
            request.addHeader("user-agent", "Twitter Stream Reader");
            request.addBodyParameter("track", searchTerm);
            service.signRequest(accessToken, request);
            Response response = request.send();

            // Create a reader to read Twitter stream
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));

            String line;
            while ((line = reader.readLine()) != null) {
            	
            	if (line.length() > 0){

            		String [] parts = line.split(",", 5);
	            	String ID = parts[1];
	            	String text = parts[3];
	            	
	            	// Concatenate ID & text, add delimiter & record separator
	            	String tweet = ID + "|" + text + "\n";
	            	
	            	System.out.println("SENDING TO S3 BUCKET: " + tweet);
	                
	                // cast tweets to byte array
	                byte [] tweetBytes = tweet.getBytes();
	                
	                // cast byte array to ByteBuffer (AWS Request data format)
	                ByteBuffer tweetBB = ByteBuffer.wrap(tweetBytes);
	
	                PutRecordRequest putRecordRequest = new PutRecordRequest();
	            	putRecordRequest.setDeliveryStreamName(firehoseName);
	
	            	Record record = new Record();
	            	record.setData(tweetBB);
	            	putRecordRequest.setRecord(record);
	            	
	            	firehose.putRecord(putRecordRequest);
	            	}
            	}
        } catch (IOException ioe){
            ioe.printStackTrace();
        }

    }
}