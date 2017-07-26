package com.microsoft.azure.datalake.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.JobExecutor.UploadStatus;

class MetaData {
	private static final char fileSeperator = '/';
	private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.FileUploader");
	String destinationPath, destinationUuidName;
	String destinationIntermediatePath = null;
	File sourceFile;
	long splits;
	AtomicLong doneCount = new AtomicLong(0);
	UploadStatus status = UploadStatus.successful;
	Boolean existsAtDestination = null;

	
	
	// Constructor called by producer. Perform all the one time operations here.
	// all other class functions are called by concurrent consumers.
	MetaData(File sourceFile, String destinationPath) {
		this.sourceFile = sourceFile;
		this.destinationPath = trimTrailingSlash(destinationPath) + fileSeperator;
		this.destinationUuidName = UUID.randomUUID().toString() + fileSeperator;
		setDestinationIntermediatePath();
	}
	
	public String getSourceFilePath() {
		return sourceFile.getAbsolutePath();
	}
	/*
	 * To avoid renaming, if the file size is less than chunkSize there is no
	 * intermediate UUID name.
	 */
	private void setDestinationIntermediatePath() {
		splits = EnumerateFile.getNumberOfFileChunks(sourceFile.length());
		String sourceFileName = sourceFile.getName();
		if(splits == 1) {
			destinationIntermediatePath = destinationPath + sourceFileName;
		} else {
			destinationIntermediatePath = (destinationPath + sourceFileName + "-segments-" 
		                                   + destinationUuidName + sourceFileName + "-");
		}
	}
	
	public String getDestinationIntermediatePath() {
		return destinationIntermediatePath;
	}
	
	public String getDestinationFinalPath() {
		return destinationPath + sourceFile.getName();
	}
	
	public boolean isSplitUpload() {
		return splits > 1;
	}
	
	public List<String> getChunkFiles() {
		List<String> list = new ArrayList<String>();
		String destination = getDestinationIntermediatePath();
		for(long jobid = 0; jobid < splits; jobid++) {
			list.add(destination + jobid);
		}
		return list;
	}
	
	public long size() {
		return sourceFile.length();
	}
	
	public boolean isFinalUpload() {
		return splits == doneCount.incrementAndGet();
	}
	
	private static String trimTrailingSlash(String inStr) {
		int i = inStr.length()-1;
		while(i >= 0 && inStr.charAt(i) == '/') {
			i--;
		}
		return inStr.substring(0, i+1);
	}

	public synchronized boolean existsAtDestination(ADLStoreClient client) {
		if(existsAtDestination != null) {
			return existsAtDestination;
		}
		try {
			existsAtDestination = client.checkExists(getDestinationFinalPath());
		} catch (IOException e) {
			log.error("Failed to check if exists, skipping upload: " + e.getMessage());
			existsAtDestination = true;
		}
		return existsAtDestination;
	}

	public synchronized void updateStatus(UploadStatus status) {
		if(UploadStatus.failed == status) {
			this.status = status;
		}
	}
	
	public synchronized UploadStatus getUploadStatus() {
		if(existsAtDestination != null && existsAtDestination == true) {
			return UploadStatus.skipped;
		}
		return status;
	}
}