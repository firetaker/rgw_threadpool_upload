package xs3_multi_demo;

import java.io.File;
import java.util.concurrent.Callable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class xs3_upload_part_callable implements Callable<Integer>{
	xs3_upload_part_callback callback;
    
    private AmazonS3 xs3_client;
	private String xs3_multi_uploadid;
	private String xs3_bucketname;
	private String xs3_objname;
	private int part_no;
	private String file_path;
	
    public xs3_upload_part_callable(AmazonS3 xs3_client, String xs3_multi_uploadid,
			String xs3_bucketname, String xs3_objname, int part_no,
			String file_path,xs3_upload_part_callback obj){
    	this.xs3_client = xs3_client;
		this.xs3_bucketname = xs3_bucketname;
		this.xs3_objname = xs3_objname;
		this.xs3_multi_uploadid = xs3_multi_uploadid;
		this.part_no = part_no;
		this.file_path = file_path;
        this.callback = obj;
    }
    
    public Integer call(){    	
        System.out.println(Thread.currentThread().getName());
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

			callback.callbackMethod(xs3_part_etag, part_no);
			System.out.println(" -- part_id: " + part_no + " Etag: "
					+ xs3_upload_res.getPartETag().getETag());
			return 0;
		} catch (AmazonServiceException ase) {
			System.out.println("xs3_svr_error_message:" + ase.getMessage());
			System.out.println("xs3_svr_status_code:  " + ase.getStatusCode());
			System.out.println("xs3_svr_error_code:   " + ase.getErrorCode());
			System.out.println("xs3_svr_error_type:   " + ase.getErrorType());
			System.out.println("xs3_svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("xs3_clt_error_message:" + ace.getMessage());
		}
		
        return 0;
    }
}