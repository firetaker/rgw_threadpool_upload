package xs3_multi_demo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class xs3_utils {

	public static String xs3_init_upload(AmazonS3 xs3_client,
			String xs3_bucketname, String xs3_objname) {
		InitiateMultipartUploadRequest xs3_multi_req = new InitiateMultipartUploadRequest(
				xs3_bucketname, xs3_objname);
		xs3_multi_req.setCannedACL(CannedAccessControlList.PublicRead);
		InitiateMultipartUploadResult xs3_multi_res = xs3_client
				.initiateMultipartUpload(xs3_multi_req);

		String xs3_multi_uploadid = xs3_multi_res.getUploadId();
		return xs3_multi_uploadid;
	}

	public static PartETag xs3_upload_part(AmazonS3 xs3_client,
			String xs3_multi_uploadid, String xs3_bucketname,
			String xs3_objname, int part_no, String file_path) {
		final int xs3_part_size = 1024 * 1024 * 5;
		File local_file = new File(file_path);

		try {
			long xs3_offset_bytes = xs3_part_size * part_no;
			// xs3_input.skip(xs3_offset_bytes);

			long part_size = xs3_part_size < (local_file.length() - xs3_offset_bytes) ? xs3_part_size
					: (local_file.length() - xs3_offset_bytes);

			UploadPartRequest xs3_upload_req = new UploadPartRequest();
			xs3_upload_req.setBucketName(xs3_bucketname);
			xs3_upload_req.setKey(xs3_objname);
			xs3_upload_req.setUploadId(xs3_multi_uploadid);
			xs3_upload_req.setFile(local_file);
			xs3_upload_req.setFileOffset(xs3_offset_bytes);
			// xs3_upload_req.setInputStream(xs3_input);
			xs3_upload_req.setPartSize(part_size);
			xs3_upload_req.setPartNumber(part_no + 1);
			UploadPartResult xs3_upload_res = xs3_client
					.uploadPart(xs3_upload_req);
			PartETag xs3_part_etag = xs3_upload_res.getPartETag();

			// xs3_input.close();
			System.out.println(" -- part_id: " + part_no + " Etag: "
					+ xs3_upload_res.getPartETag().getETag());
			return xs3_part_etag;
		} catch (AmazonServiceException ase) {
			System.out.println("xs3_svr_error_message:" + ase.getMessage());
			System.out.println("xs3_svr_status_code:  " + ase.getStatusCode());
			System.out.println("xs3_svr_error_code:   " + ase.getErrorCode());
			System.out.println("xs3_svr_error_type:   " + ase.getErrorType());
			System.out.println("xs3_svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("xs3_clt_error_message:" + ace.getMessage());
		}
		return null;
	}

	public static List<PartETag> xs3_multipart_upload_callback(
			AmazonS3 xs3_client, String xs3_multi_uploadid,
			String xs3_bucketname, String xs3_objname, String file_path) {

		final int xs3_part_size = 1024 * 1024 * 5;
		File local_file = new File(file_path);

		int xs3_part_count = (int) Math.ceil((double) (local_file.length())
				/ (double) xs3_part_size);

		try {
			ExecutorService executor = Executors.newFixedThreadPool(5);
			List<Callable<Integer>> tasks = new ArrayList<>();

			List<PartETag> xs3_part_etags = new java.util.ArrayList<PartETag>();
			for (int part_no = 0; part_no < xs3_part_count; part_no++) {

				long xs3_offset_bytes = xs3_part_size * part_no;
				long part_size = xs3_part_size < (local_file.length() - xs3_offset_bytes) ? xs3_part_size
						: (local_file.length() - xs3_offset_bytes);

				xs3_upload_part_callback cb = new xs3_upload_part_callback(
						xs3_part_etags);

				xs3_upload_part_callable myCallable = new xs3_upload_part_callable(
						xs3_client, xs3_multi_uploadid, xs3_bucketname,
						xs3_objname, part_no, file_path, cb);				
				tasks.add(myCallable);
				System.out.println(" -- part_id: " + part_no + " part_size : "
						+ part_size);
			}

			try {
				List<Future<Integer>> futures = executor.invokeAll(tasks);
				int flag = 0;

				for (Future<Integer> f : futures) {
					if (!f.isDone())
						flag = 1;
				}

				if (flag == 0)
					System.err.println("callbacks success !");
				else
					System.err.println("callbacks failed !");

			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				executor.shutdown();
			}
			System.out.println(" All threads Finished ");

			return xs3_part_etags;
		} catch (Exception ie) {
			System.err.println(ie.getMessage());
			ie.printStackTrace();
		}

		return null;
	}

	public static List<PartETag> xs3_multipart_upload(AmazonS3 xs3_client,
			String xs3_multi_uploadid, String xs3_bucketname,
			String xs3_objname, String file_path) {

		final int xs3_part_size = 1024 * 1024 * 5;
		File local_file = new File(file_path);

		int xs3_part_count = (int) Math.ceil((double) (local_file.length())
				/ (double) xs3_part_size);

		try {
			List<PartETag> xs3_part_etags = new java.util.ArrayList<PartETag>();
			for (int part_no = 0; part_no < xs3_part_count; part_no++) {

				long xs3_offset_bytes = xs3_part_size * part_no;
				long part_size = xs3_part_size < (local_file.length() - xs3_offset_bytes) ? xs3_part_size
						: (local_file.length() - xs3_offset_bytes);

				PartETag partETag = xs3_upload_part(xs3_client,
						xs3_multi_uploadid, xs3_bucketname, xs3_objname,
						part_no, file_path);
				if (null != partETag) {
					xs3_part_etags.add(partETag);
				}
				System.out.println(" -- part_id: " + part_no + " part_size : "
						+ part_size);
			}

			return xs3_part_etags;
		} catch (Exception ie) {
			System.err.println(ie.getMessage());
			ie.printStackTrace();
		}
		return null;
	}

	public static List<PartETag> xs3_list_parts(AmazonS3 xs3_client,
			String xs3_multi_uploadid, String xs3_bucketname, String xs3_objname) {
		if (null == xs3_client || null == xs3_multi_uploadid)
			return null;

		try {

			// System.setProperty("org.xml.sax.driver",
			// "org.apache.xerces.parsers.SAXParser");
			// System.setProperty("org.xml.sax.driver",
			// "javax.xml.parsers.SAXParser");
			List<PartETag> xs3_part_etags = new java.util.ArrayList<PartETag>();
			ListPartsRequest reqs = new ListPartsRequest(xs3_bucketname,
					xs3_objname, xs3_multi_uploadid);
			//reqs.setEncodingType("url");

			PartListing xs3_parts = xs3_client.listParts(reqs);

			if (null != xs3_parts.getMaxParts()) {
				for (PartSummary part : xs3_parts.getParts()) {
					System.out.println("PartNumber: " + part.getPartNumber()
							+ " ETag: " + part.getETag());
					PartETag eTag = new PartETag(part.getPartNumber(),
							part.getETag());
					xs3_part_etags.add(eTag);
				}
			}
			return xs3_part_etags;
		} catch (AmazonServiceException ase) {
			System.out.println("xs3_svr_error_message:" + ase.getMessage());
			System.out.println("xs3_svr_status_code:  " + ase.getStatusCode());
			System.out.println("xs3_svr_error_code:   " + ase.getErrorCode());
			System.out.println("xs3_svr_error_type:   " + ase.getErrorType());
			System.out.println("xs3_svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("xs3_clt_error_message:" + ace.getMessage());
			ace.printStackTrace();
		}
		return null;
	}

	public static void xs3_compelete_upload(AmazonS3 xs3_client,
			String xs3_multi_uploadid, String xs3_bucketname,
			String xs3_objname, List<PartETag> xs3_part_etags) {

		if (null == xs3_client || null == xs3_part_etags)
			return;

		try {
			CompleteMultipartUploadRequest xs3_complete_req = new CompleteMultipartUploadRequest(
					xs3_bucketname, xs3_objname, xs3_multi_uploadid,
					xs3_part_etags);

			CompleteMultipartUploadResult xs3_complete_res = xs3_client
					.completeMultipartUpload(xs3_complete_req);
			System.out.println(xs3_complete_res.getETag());

		} catch (AmazonServiceException ase) {
			System.out.println("xs3_svr_error_message:" + ase.getMessage());
			System.out.println("xs3_svr_status_code:  " + ase.getStatusCode());
			System.out.println("xs3_svr_error_code:   " + ase.getErrorCode());
			System.out.println("xs3_svr_error_type:   " + ase.getErrorType());
			System.out.println("xs3_svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("xs3_clt_error_message:" + ace.getMessage());
		}

	}

	public static void xs3_abort_upload(AmazonS3 xs3_client,
			String xs3_multi_uploadid, String xs3_bucketname, String xs3_objname) {
		if (null == xs3_client || null == xs3_multi_uploadid)
			return;

		try {
			xs3_client.abortMultipartUpload(new AbortMultipartUploadRequest(
					xs3_bucketname, xs3_objname, xs3_multi_uploadid));
		} catch (AmazonServiceException ase) {
			System.out.println("xs3_svr_error_message:" + ase.getMessage());
			System.out.println("xs3_svr_status_code:  " + ase.getStatusCode());
			System.out.println("xs3_svr_error_code:   " + ase.getErrorCode());
			System.out.println("xs3_svr_error_type:   " + ase.getErrorType());
			System.out.println("xs3_svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("xs3_clt_error_message:" + ace.getMessage());
		}
	}

}
