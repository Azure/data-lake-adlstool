package com.microsoft.azure.datalake.store;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.UploadJob.JobType;

class JobExecutor implements Runnable {
	private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.FileUploader");
	final char fileSeparator = '/';
	final int fourMB = 4 * 1024 * 1024;
	final int bufSize = fourMB;
	ConsumerQueue<UploadJob> jobQ;
	ADLStoreClient client;
	Stats stats;
	
	class Stats {
		int numberOfChunksUploaded;
		int numberOfFailedUploads;
		long totalTimeTakenInMilliSeconds;
		long totalBytesUploaded;
		List<String> successfulUploads = new LinkedList<String>();
		List<String> failedUploads = new LinkedList<>();
		
		public void begin() {
			totalTimeTakenInMilliSeconds = System.currentTimeMillis();
		}
		public void end() {
			totalTimeTakenInMilliSeconds = System.currentTimeMillis() - totalTimeTakenInMilliSeconds;
		}
		public void updateChunkStats(boolean uploadStatus, long size) {
			if(uploadStatus) {
				numberOfChunksUploaded++;
				totalBytesUploaded += size;
			} else {
				numberOfFailedUploads++;
			}
		}
		
		public long getBytesUploaded() {
			return totalBytesUploaded;
		}
		public int getFailedUploads() {
			return failedUploads.size();
		}
		public void addUploadedItem(UploadJob job, boolean status) {
			if(status) {
				successfulUploads.add(job.getSourcePath());
			} else {
				failedUploads.add(job.getSourcePath());
			}
			
		}
		public List<String> getSuccessfulUploads() {
			return successfulUploads;
		}
		public List<String> getFailedUplaods() {
			return failedUploads;
		}
	}
	
	JobExecutor(ConsumerQueue<UploadJob> jobQ, ADLStoreClient client) {
		this.jobQ = jobQ;
		this.client = client;
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
		String filePath = job.getDstUploadPath();
		boolean status = uploadFileInternal(job, filePath);
		job.updateSuccess(status);
		stats.updateChunkStats(status, job.size);
		if(job.isFinalUpload()) {
			status = false;
			if(job.fileUploadSuccess()) {
				try {
					if(concatenate(job) && verifyUpload(job)) {
						status = true;
					}
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			} else {
				log.error("Upload failed: source file path " + job.data.getSourceFilePath());
			}
			stats.addUploadedItem(job, status);
		}
	}
	
	boolean uploadFileInternal(UploadJob job, String filePath) {
		try ( OutputStream stream = client.createFile(filePath, IfExists.OVERWRITE);
		        InputStream srcData = new FileInputStream(job.data.sourceFile);)
		{
			byte[] buf = new byte[bufSize];
	        long totalBytesRead = 0;
	        int dataRead = 0;
	          
	        srcData.skip(job.offset);
	        while(totalBytesRead < job.size && 
	        		(dataRead = srcData.read(buf, 0, (int)Math.min(bufSize, job.size - totalBytesRead))) != -1) {
	        	stream.write(buf, 0, dataRead);
	            totalBytesRead += dataRead;
	        }
	        if(totalBytesRead != job.size) {
	        	log.error("Failed to upload: " + job.data.getSourceFilePath());
	           return false;
	        }
		} catch (IOException e) {
			log.error(e.getMessage());
			return false;
		}
		return true;
	}
	
	
	boolean concatenate(UploadJob job) {
		if(!job.data.isSplitUpload()) return true;
		boolean status = false;
		String finalDestination = job.getDstFinalPath();
		List<String> chunkedFiles = job.data.getChunkFiles();
		try {
			client.delete(finalDestination);
			status = client.concatenateFiles(finalDestination, chunkedFiles);
		} catch (IOException e) {
			log.error(e.getMessage());
			log.error("Concatenation failed, failed to upload: " + finalDestination);
		}
		return status;
	}
	/*
	 * Verify the upload was successful for the given file.
	 * trivial size check for now.
	 * clean up?
	 */
	boolean verifyUpload(UploadJob job) throws IOException {
		String filePath = job.getDstFinalPath();
		DirectoryEntry entry = client.getDirectoryEntry(filePath);
		if(entry.length != job.data.sourceFile.length()) {
			log.error(job.data.sourceFile.getAbsolutePath() + " final verification failed");
			return false;
		}
		log.debug(job.getSourcePath() + " verification successful");
		return true;
	}
	
	void mkDir(UploadJob job) {
		String filePath = job.getDstFinalPath();
		boolean status = false;
		try {
			status = client.createDirectory(filePath);
		} catch (IOException e) {
			log.error("Failed to create directory " + filePath);
		}
		stats.addUploadedItem(job, status);
	}
}