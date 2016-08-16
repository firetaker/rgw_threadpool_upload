package xs3_multi_demo;

import java.util.List;

import com.amazonaws.services.s3.model.PartETag;

public class xs3_upload_part_callback {

	private List<PartETag> etags;

	public xs3_upload_part_callback(List<PartETag> xs3_part_etags) {
		etags = xs3_part_etags;
	}

	synchronized public void callbackMethod(PartETag etag, int part_no) {
		System.out.println("   -Callback: " + part_no + " Etag: "
				+ etag.getETag());
		this.etags.add(etag);
	}

}
