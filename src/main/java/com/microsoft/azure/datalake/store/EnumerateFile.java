package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.Job.JobType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

;
/*
 * Enumerates the given Directory and populates the upload jobs
 */

class EnumerateFile implements Runnable {
	private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.FileUploader");
	private ProcessingQueue<MetaData> metaDataQ;
	private ConsumerQueue<Job> jobQ;
	private static long chunkSize = 64 * 1024 * 1024; // 256 MB
	private static long threshhold = 64 * 1024 * 1024; // 356 MB
	private long bytesToTransmit;
	private boolean isDownload = true;
	private ADLStoreClient client;
	private static int maxEntries = 2000;
	
	EnumerateFile(File srcDir, String destination, ProcessingQueue<MetaData> metaDataQ, ConsumerQueue<Job> jobQ) {
		this.metaDataQ = metaDataQ;
		this.jobQ = jobQ;
		this.isDownload = false;
		long size = AdlsTool.getChunkSize(chunkSize);
		if(size != chunkSize) {
			chunkSize = size;
			threshhold = size;
		}
		metaDataQ.add(new MetaData(srcDir, destination));
	}
	
	EnumerateFile(DirectoryEntry source, String destination, 
			      ProcessingQueue<MetaData> metaDataQ, ConsumerQueue<Job> jobQ, ADLStoreClient client) {
		this.metaDataQ = metaDataQ;
		this.jobQ = jobQ;
		this.isDownload = true;
		long size = AdlsTool.getChunkSize(chunkSize);
		if(size != chunkSize) {
			chunkSize = size;
			threshhold = size;
		}
		this.client = client;
		metaDataQ.add(new MetaData(source, destination));
	}
	public void run() {
		if(isDownload) {
			enumerateAdlsFiles();
		} else {
			enumerateLocalFiles();
		}
	}
	
	private void enumerateAdlsFiles() {
		MetaData front;
		while((front = metaDataQ.poll()) != null) {
			DirectoryEntry source = front.sourceEntry;
			if(source.type == DirectoryEntryType.DIRECTORY) {
				try {
					List<DirectoryEntry> subDir;
					String lastEntry = null;
					do {
						subDir = client.enumerateDirectory(source.fullName, maxEntries, lastEntry);
						String dstPrefix = front.getDestinationFinalPath();
						for(DirectoryEntry dEntry: subDir) {
							lastEntry = dEntry.name;
							metaDataQ.add(new MetaData(dEntry, dstPrefix));
						}
					} while(subDir.size() >= maxEntries);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if(source.type == DirectoryEntryType.FILE) {
				generateDownloadFileJob(front);
			}
			metaDataQ.unregister();
		}
	}
	
	
	private void enumerateLocalFiles() {
		MetaData front;
		while((front = metaDataQ.poll()) != null) {
			try {
				File source = front.sourceFile;
				if(source.isDirectory()) {
					File[] subDir = source.listFiles();
					if(subDir != null) {
						String dstPrefix = front.getDestinationFinalPath();
						for(File sub : subDir) {
							metaDataQ.add(new MetaData(sub, dstPrefix));
						}
						if(subDir.length == 0) {
							generateMkDirJob(front);
						}
					}
				} else if(source.isFile()) {
					generateUploadJob(front);
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			} finally {
				metaDataQ.unregister();
			}
		}
	}
	
	private void generateMkDirJob(MetaData front) {
		jobQ.add(new Job(front, 0, 0, 0, JobType.MKDIR));
	}
	
	private void generateUploadJob(MetaData front) {
		long size = 0, chunks = 0, offset = 0;
		do {
			if(front.size() - offset <= threshhold) {
				size = front.size() - offset;
			} else {
				size = chunkSize;
			}
			jobQ.add(new Job(front, offset, size, chunks, JobType.FILEUPLOAD));
			chunks++;
			offset += size;
		} while(offset < front.size());
		log.debug("Generated " + front.splits + " number of upload jobs for file " 
				+ front.getSourceFilePath() + " with destination " + front.getDestinationIntermediatePath());
		bytesToTransmit += front.size();
	}
	
	private void generateDownloadFileJob(MetaData entry) {
		long size = 0, chunks = 0, offset = 0;
		long totalLength = entry.sourceEntry.length;
		do {
			if(totalLength - offset <= threshhold) {
				size = totalLength - offset;
			} else {
				size = chunkSize;
			}
			jobQ.add(new Job(entry, offset, size, chunks, JobType.FILEDOWNLOAD));
			chunks++;
			offset += size;
		} while(offset < totalLength);
		bytesToTransmit += totalLength;
		log.debug("Generated " + chunks + " number of downlaod jobs for size: " + totalLength);
	}
	
	public long getBytesToTransmit() {
		return bytesToTransmit;
	}
	
	static long getNumberOfFileChunks(long size) {
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
}