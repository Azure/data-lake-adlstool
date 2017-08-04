package com.microsoft.azure.datalake.store;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.UploadJob.JobType;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;

class JobExecutor implements Runnable {
	private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.FileUploader");
	final char fileSeparator = '/';
	final int fourMB = 4 * 1024 * 1024;
	final int bufSize = fourMB;
	ConsumerQueue<UploadJob> jobQ;
	ADLStoreClient client;
	Stats stats;
	IfExists overwrite;
	
	static enum UploadStatus {
		successful,
		failed,
		skipped
	}
	
	class Stats {
		int numberOfChunksUploaded;
		int numberOfFailedUploads;
		long totalTimeTakenInMilliSeconds = 0;
		AtomicLong totalBytesUploaded = new AtomicLong(0);
		List<String> successfulUploads = new LinkedList<String>();
		List<String> failedUploads = new LinkedList<>();
		List<String> skippedUploads = new LinkedList<>();
		
		public void begin() {
			totalTimeTakenInMilliSeconds = System.currentTimeMillis();
		}
		public void end() {
			totalTimeTakenInMilliSeconds = System.currentTimeMillis() - totalTimeTakenInMilliSeconds;
		}
		public void updateChunkStats(UploadStatus status, long size) {
			if(status == UploadStatus.successful) {
				numberOfChunksUploaded++;
				totalBytesUploaded.addAndGet(size);
			} else if(status == UploadStatus.failed){
				numberOfFailedUploads++;
			}
		}
		
		public long getBytesUploaded() {
			return totalBytesUploaded.get();
		}
		
		public void addUploadedItem(UploadJob job, UploadStatus status) {
			if(status == UploadStatus.successful) {
				successfulUploads.add(job.getSourcePath());
			} else if(status == UploadStatus.failed){
				failedUploads.add(job.getSourcePath());
			} else {
				skippedUploads.add(job.getSourcePath());
			}
			
		}
		public List<String> getSuccessfulUploads() {
			return successfulUploads;
		}
		public List<String> getFailedUploads() {
			return failedUploads;
		}
		public List<String> getSkippedUploads() {
			return skippedUploads;
		}
	}
	
	JobExecutor(ConsumerQueue<UploadJob> jobQ, ADLStoreClient client, IfExists overwrite) {
		this.jobQ = jobQ;
		this.client = client;
		this.overwrite = overwrite;
		stats = new Stats();
	}
	
	public void run() {
		UploadJob job;
		stats.begin();
		while((job = jobQ.poll()) != null) {
			if(job.type == JobType.MKDIR) {
				mkDir(job);
			} else if(job.type == JobType.FILEUPLOAD){
				uploadFile(job);
			}
		}
		stats.end();
	}
	
	void uploadFile(UploadJob job){
		UploadStatus status = uploadFileInternal(job);
		job.updateStatus(status);
		stats.updateChunkStats(status, job.size);
		if(job.isFinalUpload()) {
			status = job.fileUploadSuccess();
			if(status == UploadStatus.successful) {
				try {
					if(!(concatenate(job) && verifyUpload(job))) {
						status = UploadStatus.failed;
					}
				} catch (IOException e) {
					status = UploadStatus.failed;
					log.error(e.getMessage());
				}
			}
			
			if(status == UploadStatus.failed){
				log.error("Upload failed: source file path " + job.getSourcePath());
			} else if(status == UploadStatus.skipped){
				log.debug("Upload Skipped: source file path " + job.getSourcePath());
			}
			stats.addUploadedItem(job, status);
		}
	}
	
	boolean skipUpload(UploadJob job) {
		if(overwrite == IfExists.OVERWRITE) {
			// overwrite option is provided by the user. Proceed to upload the file.
			return false;
		} else {
			// check to see if there is already a file with same name at the destination.
			return job.existsAtDestination(client);
		}
	}
	
	private UploadStatus uploadFileInternal(UploadJob job) {
		if(skipUpload(job)) {
			return UploadStatus.skipped;
		}
		String filePath = job.getDestinationIntermediatePath();
		try ( ADLFileOutputStream stream = client.createFile(filePath, IfExists.OVERWRITE);
				FileInputStream srcData = new FileInputStream(job.getSourcePath());)
		{
			srcData.skip(job.offset);
			byte[] data = new byte[bufSize];
	        long totalBytesRead = 0;
	        long dataRead = 0;
	        while(totalBytesRead < job.size && (dataRead = srcData.read(data)) != -1) {
	        	int len = (int)Math.min(dataRead, job.size - totalBytesRead);
	        	stream.write(data, 0, len);
	        	totalBytesRead += len;
	        }
	        if(totalBytesRead != job.size) {
	           log.error("Failed to upload: " + job.data.getSourceFilePath());
	           return UploadStatus.failed;
	        }
		} catch (IOException e) {
			log.error(e.getMessage());
			return UploadStatus.failed;
		}
		return UploadStatus.successful;
	}
	
	
	boolean concatenate(UploadJob job) {
		if(!job.data.isSplitUpload()) return true;
		boolean status = false;
		String finalDestination = job.getDestinationFinalPath();
		String intermediatePath = job.data.getDestinationConcatIntermediatePath();
		List<String> chunkedFiles = job.data.getChunkFiles();
		
		try {
			status = concatenateCall(intermediatePath, chunkedFiles, client);
			if(status) {
				status = client.rename(intermediatePath, finalDestination, true);
			}
		} catch (IOException e) {
			log.error(e.getMessage());
			log.error("Concatenation failed, failed to upload: " + finalDestination);
		}
		return status;
	}
	
	boolean concatenateCall(String path, List<String> streams, ADLStoreClient client) {
		RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialBackoffPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concat(path, streams, client, true, opts, resp);
        return resp.successful;
	}
	/*
	 * Verify the upload was successful for the given file.
	 * trivial size check for now.
	 * clean up?
	 */
	boolean verifyUpload(UploadJob job) throws IOException {
		String filePath = job.getDestinationFinalPath();
		DirectoryEntry entry = client.getDirectoryEntry(filePath);
		if(entry.length != job.data.sourceFile.length()) {
			log.error(job.data.sourceFile.getAbsolutePath() + " final verification failed");
			return false;
		}
		log.debug(job.getSourcePath() + " verification successful");
		return true;
	}
	
	void mkDir(UploadJob job) {
		String filePath = job.getDestinationFinalPath();
		UploadStatus status = UploadStatus.failed;
		try {
			if(client.createDirectory(filePath)) {
				status = UploadStatus.successful;
			}
		} catch (IOException e) {
			log.error("Failed to create directory " + filePath);
		}
		stats.addUploadedItem(job, status);
	}
}