package com.microsoft.azure.datalake.store;

import java.util.LinkedList;
import java.util.List;

public class UploadResult {
	public long timeTakenInMilliSeconds = 0;
	long totalSizeInBytes;
	public List<String> successfulUploads = new LinkedList<>();
	public List<String> failedUploads = new LinkedList<>();
	public List<String> skippedUploads = new LinkedList<>();
	public void update(JobExecutor.Stats stats) {
		totalSizeInBytes += stats.getBytesTransferred();
		successfulUploads.addAll(stats.getSuccessfulUploads());
		failedUploads.addAll(stats.getFailedUploads());
		skippedUploads.addAll(stats.getSkippedUploads());
		timeTakenInMilliSeconds = Math.max(timeTakenInMilliSeconds, stats.totalTimeTakenInMilliSeconds);
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
