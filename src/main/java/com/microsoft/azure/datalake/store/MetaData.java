package com.microsoft.azure.datalake.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

class MetaData {
	static final char fileSeperator = '/';
	static final int threshhold = EnumerateFile.threshhold;
	static final int chunkSize = EnumerateFile.chunkSize;
	String destinationPath, destinationUuidName, sourceFileName;
	String destinationIntermediatePath;
	File sourceFile;
	long splits;
	AtomicLong doneCount = new AtomicLong(0);
	boolean uploadSuccessful = true;
	MetaData(File sourceFile, String destinationPath) {
		this.sourceFile = sourceFile;
		this.destinationPath = trimTrailingSlash(destinationPath) + fileSeperator;
		this.destinationUuidName = UUID.randomUUID().toString() + fileSeperator;
		sourceFileName = sourceFile.getName();
		splits = getNumberOfFileChunks(sourceFile.length());
		if(splits == 1) {
			destinationIntermediatePath = this.destinationPath + sourceFileName;
		} else {
			destinationIntermediatePath = this.destinationPath + destinationUuidName + sourceFileName + "-";
		}
	}
	
	private static long getNumberOfFileChunks(long size) {
		if(size <= threshhold) {
			return 1;
		}
		long chunks = 0;
		if(size%chunkSize <= (threshhold-chunkSize)) {
			chunks = size/chunkSize;
		} else {
			chunks = (long)Math.ceil(1.0*size/chunkSize);
		}
		return chunks;
	}
	
	public String getSourceFilePath() {
		return sourceFile.getAbsolutePath();
	}
	/*
	 * To avoid renaming, if the file size is less than chunkSize there is no
	 * intermediate UUID name.
	 */
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
	
	private static String trimTrailingSlash(String inStr) {
		int i = inStr.length()-1;
		while(i >= 0 && inStr.charAt(i) == '/') {
			i--;
		}
		return inStr.substring(0, i+1);
	}
}