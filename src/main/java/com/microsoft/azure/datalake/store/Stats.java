package com.microsoft.azure.datalake.store;

import java.util.ArrayList;
import java.util.List;

public class Stats {
	public long timeTakenInMilliSeconds = 0;
	long totalSizeInBytes;
	public List<String> successfulTransfers = new ArrayList<>();
	public List<String> failedTransfers = new ArrayList<>();
	public List<String> skippedTransfers = new ArrayList<>();
	public void update(JobExecutor.Stats stats) {
		totalSizeInBytes += stats.getBytesTransferred();
		successfulTransfers.addAll(stats.getSuccessfulUploads());
		failedTransfers.addAll(stats.getFailedUploads());
		skippedTransfers.addAll(stats.getSkippedUploads());
		timeTakenInMilliSeconds = Math.max(timeTakenInMilliSeconds, stats.totalTimeTakenInMilliSeconds);
	}
	public List<String> getSuccessfulTransfers() {
		return successfulTransfers;
	}
	public List<String> getFailedTransfers() {
		return failedTransfers;
	}
	public List<String> getSkippedTransfers() {
		return skippedTransfers;
	}
}
