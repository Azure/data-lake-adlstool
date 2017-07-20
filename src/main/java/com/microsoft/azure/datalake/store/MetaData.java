package com.microsoft.azure.datalake.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

class MetaData {
	static final char fileSeperator = '/';
	String destinationPath, destinationUuidName, sourceFileName;
	File sourceFile;
	long splits;
	AtomicLong doneCount = new AtomicLong(0);
	boolean uploadSuccessful = true;
	MetaData(File sourceFile, String destinationPath) {
		this.sourceFile = sourceFile;
		this.destinationPath = trimTrailingSlash(destinationPath) + fileSeperator;
		destinationUuidName = UUID.randomUUID().toString() + fileSeperator;
		sourceFileName = sourceFile.getName();
	}
	
	public String getSourceFileName() {
		return sourceFile.getAbsolutePath();
	}
	/*
	 * To avoid renaming, if the file size is less than chunkSize there is no
	 * intermediate UUID name.
	 */
	public String getDstUploadPath() {
		if(splits == 1) {
			return destinationPath + sourceFileName;
		} else {
			return destinationPath + destinationUuidName + sourceFileName + "-";
		}
	}
	
	public String getDstFinalPath() {
		return destinationPath + sourceFile.getName();
	}
	
	public boolean isSplitUpload() {
		return splits > 1;
	}
	
	public List<String> getChunkFiles() {
		List<String> list = new ArrayList<String>();
		String destination = getDstUploadPath();
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