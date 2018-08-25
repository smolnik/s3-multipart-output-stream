package com.adamsmolnik;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3ZipperSample {

	private static final int DATA_SIZE = 20 * 1024 * 1024;

	public static void main(String[] args) throws Exception {
		AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		InputStream is = generateRandomData();
		long then = System.currentTimeMillis();
		try (ZipOutputStream zos = new ZipOutputStream(
				new S3ParallelMultipartOutputStream(6 * 1024 * 1024, 3, "z-mutliupload-test", "data.zip", s3))) {
			zos.putNextEntry(new ZipEntry("data.dat"));
			copy(is, zos);
			zos.closeEntry();
		}
		System.out.println(System.currentTimeMillis() - then);
	}

	private static InputStream generateRandomData() {
		byte[] input = new byte[DATA_SIZE];
		new Random().nextBytes(input);
		return new ByteArrayInputStream(input);
	}

	private static final int BUFFER_SIZE = 8 * 1024;

	private static long copy(InputStream in, OutputStream out) throws IOException {
		byte[] buf = new byte[BUFFER_SIZE];
		long count = 0;
		int n = 0;
		while ((n = in.read(buf)) > -1) {
			out.write(buf, 0, n);
			count += n;
		}
		return count;
	}

}
