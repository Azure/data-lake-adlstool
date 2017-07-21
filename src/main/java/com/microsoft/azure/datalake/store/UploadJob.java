package com.microsoft.azure.datalake.store;

class UploadJob implements Comparable<UploadJob>{
	MetaData data;
	long offset, id, size;
	enum JobType {
		MKDIR, FILEUPLOAD, CONCATENATE, VERIFY
	}
	JobType type;
	UploadJob(MetaData data, long offset, long size, long id, JobType type) {
		this.data = data;
		this.offset = offset;
		this.size = size;
		this.id = id;
		this.type = type;
	}

	public int compareTo(UploadJob that) {
		return Long.compare(that.size, this.size);
	}
	
	public boolean isFinalUpload() {
		return data.splits == data.doneCount.incrementAndGet();
	}
	
	public boolean fileUploadSuccess() {
		return data.uploadSuccessful;
	}
	
	public void updateSuccess(boolean success) {
		data.uploadSuccessful &= success;
	}
	
	public String getDestinationIntermediatePath() {
		return data.getDestinationIntermediatePath() + (data.splits > 1 ? id : "");
	}
	
	public String getDestinationFinalPath() {
		return data.getDestinationFinalPath();
	}
	
	public String getSourcePath() {
		return data.sourceFile.getAbsolutePath();
	}
}