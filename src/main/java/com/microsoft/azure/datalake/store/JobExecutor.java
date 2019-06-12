package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.Job.JobType;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class JobExecutor implements Runnable {
	private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.FileUploader");
	final int fourMB = 4 * 1024 * 1024;
	final int bufSize = fourMB;
	ConsumerQueue<Job> jobQ;
	ADLStoreClient client;
	Stats stats;
	IfExists overwrite;
	
	enum UploadStatus {
		successful,
		failed,
		skipped
	}
	
	class Stats {
		int numberOfChunksUploaded;
		int numberOfFailedUploads;
		long totalTimeTakenInMilliSeconds = 0;
		AtomicLong totalBytesTransmitted = new AtomicLong(0);
		List<String> successfulTransfers = new ArrayList<>();
		List<String> failedTransfers =new ArrayList<>();
		List<String> skippedTransfers = new ArrayList<>();
		
		public void begin() {
			totalTimeTakenInMilliSeconds = System.currentTimeMillis();
		}
		public void end() {
			totalTimeTakenInMilliSeconds = System.currentTimeMillis() - totalTimeTakenInMilliSeconds;
		}
		public void updateChunkStats(UploadStatus status, long size) {
			if(status == UploadStatus.successful) {
				numberOfChunksUploaded++;
				totalBytesTransmitted.addAndGet(size);
			} else if(status == UploadStatus.failed){
				numberOfFailedUploads++;
			}
		}
		
		public long getBytesTransferred() {
			return totalBytesTransmitted.get();
		}
		
		public void addUploadedItem(Job job, UploadStatus status) {
			if(status == UploadStatus.successful) {
				successfulTransfers.add(job.getSourcePath());
			} else if(status == UploadStatus.failed){
				failedTransfers.add(job.getSourcePath());
			} else {
				skippedTransfers.add(job.getSourcePath());
			}
			
		}
		public List<String> getSuccessfulUploads() {
			return successfulTransfers;
		}
		public List<String> getFailedUploads() {
			return failedTransfers;
		}
		public List<String> getSkippedUploads() {
			return skippedTransfers;
		}
	}
	
	JobExecutor(ConsumerQueue<Job> jobQ, ADLStoreClient client, IfExists overwrite) {
		this.jobQ = jobQ;
		this.client = client;
		this.overwrite = overwrite;
		stats = new Stats();
	}
	
	public void run() {
		Job job;
		stats.begin();
		while((job = jobQ.poll()) != null) {
			if(job.type == JobType.MKDIR) {
				mkDir(job);
			} else if(job.type == JobType.FILEUPLOAD){
				uploadFile(job);
			} else if(job.type == JobType.FILEDOWNLOAD) {
				downloadFile(job);
			}
		}
		stats.end();
		log.debug("Done uploading file");
	}
	
	void downloadFile(Job job) {
		UploadStatus status = downloadFileInternal(job);
		job.updateStatus(status);
		stats.updateChunkStats(status, job.size);
		if(job.isFinalUpload()) {
			if(!skipDownload(job)) {
				status = job.fileUploadStatus();
			}
			if(status == UploadStatus.successful) {
				if(!(renameLocalFile(job) && verifyDownload(job))) {
					status = UploadStatus.failed;
				}
			}
			if(status == UploadStatus.failed){
				log.error("Download failed: source file path " + job.getSourcePath());
			} else if(status == UploadStatus.skipped){
				log.debug("Downloadload Skipped: source file path " + job.getSourcePath());
			}
			stats.addUploadedItem(job, status);
		}
	}
	
	boolean renameLocalFile(Job job) {
		Path source = Paths.get(job.data.destinationIntermediatePath);
		Path destination = Paths.get(job.data.destinationFinalPath);
		try {
			source = Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
			return Files.isSameFile(source, destination);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	UploadStatus downloadFileInternal(Job job) {
		if(skipDownload(job)) {
			return UploadStatus.skipped;
		}
		File file = job.data.destinationIntermediateFile;
		
		try ( ADLFileInputStream stream = client.getReadStream(job.getSourcePath());
				RandomAccessFile fileStream = new RandomAccessFile(file, "rw")) {
			stream.seek(job.offset);
			fileStream.seek(job.offset);
			byte[] data = new byte[bufSize];
			long totalBytesRead = 0, bytesRead;
			while(totalBytesRead < job.size && (bytesRead = stream.read(data)) != -1) {
				int write = (int) Math.min(bytesRead, job.size - totalBytesRead);
				totalBytesRead += write;
				fileStream.write(data, 0, write);
			}
		} catch (IOException e) {
			log.error(e.getMessage());
			log.error("Error downloading file " + job.getSourcePath());
		}
		return UploadStatus.successful;
	}
	
	
	void uploadFile(Job job){
		UploadStatus status = uploadFileInternal(job);
		job.updateStatus(status);
		stats.updateChunkStats(status, job.size);
		if(job.isFinalUpload()) {
			status = job.fileUploadStatus();
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
	
	boolean skipUpload(Job job) {
		if(overwrite == IfExists.OVERWRITE) {
			// overwrite option is provided by the user. Proceed to upload the file.
			return false;
		} else {
			// check to see if there is already a file with same name at the destination.
			return job.existsAtDestination(client);
		}
	}
	
	boolean skipDownload(Job job) {
		if(overwrite == IfExists.OVERWRITE) {
			// overwrite option is provided by the user. Proceed to download the file.
			return false;
		} else {
			// check to see if there is already a file with same name at the destination.
			return job.data.destinationFile.exists();
		}
	}
	
	private UploadStatus uploadFileInternal(Job job) {
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
	
	
	boolean concatenate(Job job) {
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
	boolean verifyUpload(Job job) throws IOException {
		String filePath = job.getDestinationFinalPath();
		DirectoryEntry entry = client.getDirectoryEntry(filePath);
		if(entry.length != job.data.sourceFile.length()) {
			log.error(job.data.sourceFile.getAbsolutePath() + " final verification failed");
			return false;
		}
		log.debug(job.getSourcePath() + " verification successful");
		return true;
	}
	
	boolean verifyDownload(Job job) {
		return job.data.destinationFile.length() == job.data.sourceEntry.length;
	}
	
	void mkDir(Job job) {
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