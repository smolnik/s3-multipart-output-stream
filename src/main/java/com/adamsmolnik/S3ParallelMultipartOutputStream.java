package com.adamsmolnik;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class S3ParallelMultipartOutputStream extends OutputStream {

	private static final int MIN_BUFFER_SIZE = 5 * 1024 * 1024;

	private final AmazonS3 s3;

	private final ExecutorService es;

	private final String bucket, key;

	private final String uploadId;

	private final List<Future<PartETag>> partETagFutures = new ArrayList<>();

	private AtomicInteger count = new AtomicInteger(0);

	private int partNumber = 1;

	private byte buf[];

	public S3ParallelMultipartOutputStream(String bucket, String key, AmazonS3 s3) {
		this(256 * 1024 * 1024, Runtime.getRuntime().availableProcessors(), bucket, key, s3);
	}

	/**
	 * Creates a new s3 multipart output stream, with a buffer capacity of the
	 * specified size, in bytes.
	 *
	 * @param bufferSize
	 *            the initial size.
	 * @exception IllegalArgumentException
	 *                if size is too small.
	 */
	public S3ParallelMultipartOutputStream(int bufferSize, int numberOfUploaders, String bucket, String key,
			AmazonS3 s3) {
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
		this.es = Executors.newFixedThreadPool(numberOfUploaders);
	}

	@Override
	public synchronized void write(int b) {
		write(new byte[] { (byte) b }, 0, 1);
	}

	@Override
	public synchronized void write(byte b[], int off, int len) {
		if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
			throw new IndexOutOfBoundsException();
		}
		try {
			checkBufferCapacityAndUploadOnceFull(b, off, len);
		} catch (Exception e) {
			abortUpload();
			throw new IllegalStateException(e);
		}

		int c = count.get();
		System.arraycopy(b, off, buf, c, len);
		count.set(c + len);
	}

	private synchronized void checkBufferCapacityAndUploadOnceFull(byte b[], int off, int len) {
		if (count.get() + len > buf.length) {
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
		int c = count.get();
		UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucket).withKey(key)
				.withUploadId(uploadId).withPartNumber(partNumber).withInputStream(new ByteArrayInputStream(buf, 0, c))
				.withPartSize(c).withLastPart(lastPart);
		partETagFutures.add(es.submit(() -> {
			try {
				UploadPartResult uploadResult = s3.uploadPart(uploadRequest);
				return uploadResult.getPartETag();
			} catch (Exception e) {
				abortUpload();
				throw new IllegalStateException(e);
			}
		}));
	}

	private void abortUpload() {
		s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
	}

	public synchronized void reset() {
		count = new AtomicInteger(0);
		buf = new byte[buf.length];
	}

	public void close() throws IOException {
		try {
			uploadLastPart();
			List<PartETag> partETags = partETagFutures.stream().map(t -> {
				try {
					return t.get();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}).collect(Collectors.toList());
			s3.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags));
		} finally {
			es.shutdownNow();
		}
	}
}
