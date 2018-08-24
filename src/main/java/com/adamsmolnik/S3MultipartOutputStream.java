package com.adamsmolnik;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public final class S3MultipartOutputStream extends OutputStream {

	private static final int MIN_BUFFER_SIZE = 5 * 1024 * 1024;

	private final byte buf[];

	private final AmazonS3 s3;

	private final String bucket, key;

	private final String uploadId;

	private final List<PartETag> partETags = new ArrayList<>();

	private int count = 0;

	private int partNumber = 1;

	public S3MultipartOutputStream(String bucket, String key, AmazonS3 s3) {
		this(256 * 1024 * 1024, bucket, key, s3);
	}

	/**
	 * Creates a new s3 multipart output stream, with a buffer capacity of the
	 * specified size, in bytes.
	 *
	 * @param bufferSize
	 *            the initial size.
	 * @exception IllegalArgumentException
	 *                if size is negative.
	 */
	public S3MultipartOutputStream(int bufferSize, String bucket, String key, AmazonS3 s3) {
		if (bufferSize < MIN_BUFFER_SIZE) {
			throw new IllegalArgumentException(
					"Param bufferSize is smaller than the minimum allowed size: " + MIN_BUFFER_SIZE);
		}
		this.buf = new byte[bufferSize];
		this.bucket = bucket;
		this.key = key;
		this.s3 = s3;
		InitiateMultipartUploadResult initResponse = s3
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key));
		this.uploadId = initResponse.getUploadId();
	}

	public synchronized void write(int b) {
		write(new byte[] { (byte) b }, 0, 1);
	}

	public synchronized void write(byte b[], int off, int len) {
		if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
			throw new IndexOutOfBoundsException();
		}
		try {
			checkBufferCapacityAndUploadOnceFull(b, off, len);
		} catch (Exception e) {
			s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
			throw new IllegalStateException(e);
		}

		System.arraycopy(b, off, buf, count, len);
		count += len;
	}

	private void checkBufferCapacityAndUploadOnceFull(byte b[], int off, int len) {
		if (count + len > buf.length) {
			uploadPart();
			reset();
			partNumber++;
		}

	}

	private void uploadPart() {
		upload(false);
	}

	private void uploadLastPart() {
		upload(true);
	}
	
	private void upload(boolean lastPart) {
		UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucket).withKey(key)
				.withUploadId(uploadId).withPartNumber(partNumber)
				.withInputStream(new ByteArrayInputStream(buf, 0, count)).withPartSize(count).withLastPart(lastPart);
		UploadPartResult uploadResult = s3.uploadPart(uploadRequest);
		partETags.add(uploadResult.getPartETag());
	}

	public synchronized void reset() {
		count = 0;
	}

	public synchronized int count() {
		return count;
	}

	public void close() throws IOException {
		uploadLastPart();
		s3.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags));
	}
}
