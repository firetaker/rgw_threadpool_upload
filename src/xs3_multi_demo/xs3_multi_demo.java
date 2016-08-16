package xs3_multi_demo;

import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.PartETag;

public class xs3_multi_demo {

	public static String xs3_access_key = "your access key";
	public static String xs3_secret_key = "your secrect key";
	public static String xs3_endpoint = "s3.bj.xs3cnc.com";
	public static String xs3_bucketname = "your bucket name";

	static AmazonS3 xs3_client;

	public static void main(String[] args) {
		AWSCredentials xs3_credentials = new BasicAWSCredentials(
				xs3_access_key, xs3_secret_key);
		ClientConfiguration xs3_clientconfig = new ClientConfiguration();
		xs3_clientconfig.setProtocol(Protocol.HTTP);
		xs3_clientconfig.setConnectionTimeout(300);
		xs3_clientconfig.setMaxErrorRetry(3);
		xs3_clientconfig.setMaxConnections(10);

		S3ClientOptions xs3_client_options = new S3ClientOptions();
		xs3_client_options.setPathStyleAccess(true);

		xs3_client = new AmazonS3Client(xs3_credentials, xs3_clientconfig);
		xs3_client.setEndpoint(xs3_endpoint);
		xs3_client.setS3ClientOptions(xs3_client_options);

		// init upload
		String xs3_object_name = "haha.tar.gz";
		String xs3_uploadId = xs3_utils.xs3_init_upload(xs3_client,
				xs3_bucketname, xs3_object_name);
		System.err.println(xs3_uploadId);

        // upload parts 		
		List<PartETag> etags = xs3_utils.xs3_multipart_upload_callback(
				xs3_client, xs3_uploadId, xs3_bucketname, xs3_object_name,
				"f:/Django-1.8.3.tar.gz");
	
		// compelte or abort upload 
		if (!etags.isEmpty()) {
			xs3_utils.xs3_compelete_upload(xs3_client, xs3_uploadId,
					xs3_bucketname, xs3_object_name, etags);
		} else {
			System.err.println("etags is empty");
			xs3_utils.xs3_abort_upload(xs3_client, xs3_uploadId,
					xs3_bucketname, xs3_object_name);
		}

	}

}
