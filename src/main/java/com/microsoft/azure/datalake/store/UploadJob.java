package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.JobExecutor.UploadStatus;

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
		return data.isFinalUpload();
	}
	
	public UploadStatus fileUploadSuccess() {
		return data.getUploadStatus();
	}
	
	public boolean existsAtDestination(ADLStoreClient client) {
		return data.existsAtDestination(client);
	}
	
	public String getDestinationIntermediatePath() {
		if(data.splits > 1) {
			return data.getDestinationIntermediatePath() + id;
		}
		return data.getDestinationIntermediatePath();
	}
	
	public String getDestinationFinalPath() {
		return data.getDestinationFinalPath();
	}
	
	public String getSourcePath() {
		return data.getSourceFilePath();
	}

	public void updateStatus(UploadStatus status) {
		if(UploadStatus.failed == status) {
			data.updateStatus(status);
		}
	}
}