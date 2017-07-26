package com.microsoft.azure.datalake.store;

import java.util.LinkedList;
import java.util.List;

public class UploadResult {
	boolean success = false;
	public long timeTakenInMilliSeconds = 0;
	long totalSizeInBytes;
	public List<String> successfulUploads = new LinkedList<>();
	public List<String> failedUplaods = new LinkedList<>();
	public void update(JobExecutor.Stats stats) {
		totalSizeInBytes += stats.getBytesUploaded();
		successfulUploads.addAll(stats.getSuccessfulUploads());
		failedUplaods.addAll(stats.getFailedUploads());
		timeTakenInMilliSeconds = Math.max(timeTakenInMilliSeconds, stats.totalTimeTakenInMilliSeconds);
	}
	List<String> getSuccessfulUploads() {
		return successfulUploads;
	}
	List<String> getFailedUploads() {
		return failedUplaods;
	}
}
