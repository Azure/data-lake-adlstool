package com.microsoft.azure.datalake.store;


import java.io.File;
import java.io.IOException;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;


public class FileUploader {
	private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.FileUploader");
	private static int uploaderThreadCount = 512;
	private ProcessingQueue<MetaData> metaDataQ;
	private ConsumerQueue<UploadJob> jobQ;
	private ADLStoreClient client;
	private Thread[] executorThreads;
	private JobExecutor[] executor;
	
	/*
	 * Uploads the given source dir/file to a directory on ADLS.
	 * @param source Local Source directory or file to upload from.
	 * @param destination Destination directory to copy the files to.
	 * @param client ADLStoreClient to use to upload the file.
	 */
	public static UploadResult upload(String source, String destination, ADLStoreClient client) throws IOException, InterruptedException {
		FileUploader F = new FileUploader();
		return F.uploadInternal(source, destination, client);
	}
	
	private UploadResult uploadInternal(String source, String destination, ADLStoreClient client) throws InterruptedException, IOException {
		if(source == null) {
			throw new IllegalArgumentException("source is null");
		} else if(destination == null) {
			throw new IllegalArgumentException("destination is null");
		} else if(client == null) {
			throw new IllegalArgumentException("client is null");
		}
		source = source.trim();
		destination = destination.trim();
		if(source.isEmpty()) {
			throw new IllegalArgumentException("source is empty");
		} else if(destination.isEmpty()) {
			throw new IllegalArgumentException("destination is empty");
		}
		
		File srcDir = new File(source);
		if(!srcDir.exists()) {
			throw new IllegalArgumentException("Source doesn't exist");
		}
		initialize(client);
		
		if(srcDir.isFile()) {
			if(!verifyDestination(destination)) {
				return new UploadResult();
			}
		}
		return upload(srcDir, destination);
	}
	
	
	
	
	private void initialize(ADLStoreClient client) {
		metaDataQ = new ProcessingQueue<MetaData>();
		jobQ = new ConsumerQueue<UploadJob>(new PriorityQueue<UploadJob>());
		this.client = client;
	}
	
	private void startUploaderThreads(ConsumerQueue<UploadJob> jobQ) {
		executorThreads = new Thread[uploaderThreadCount];
		executor = new JobExecutor[uploaderThreadCount];
		for(int i = 0; i < executorThreads.length; i++) {
			executor[i] = new JobExecutor(jobQ, client);
			executorThreads[i] = new Thread(executor[i]);
			executorThreads[i].start();
		}
	}
	
	private Thread startEnumeration(File source, String destination) {
		EnumerateFile jobGen = new EnumerateFile(source, destination, metaDataQ, jobQ);
		Thread t = new Thread(jobGen);
		t.start();
		return t;
	}
	
	private UploadResult upload(File source, String destination) throws IOException, InterruptedException {
		Thread generateJob = startEnumeration(source, destination);
		startUploaderThreads(jobQ);
		waitForCompletion(generateJob);
		return joinUploaderThreads();
	}
	
	private void waitForCompletion(Thread generateJob) throws InterruptedException {
		generateJob.join();
		jobQ.markComplete(); // Consumer threads wait until enumeration is active.
	}
	
	private UploadResult joinUploaderThreads() throws InterruptedException {
		UploadResult result = new UploadResult();
		for(int i = 0; i < executorThreads.length; i++) {
			executorThreads[i].join();
			result.update(executor[i].stats);
		}
		result.success = true;
		return result;
	}

	private boolean verifyDestination(String dst) throws InterruptedException {
		DirectoryEntry D = null;
		try {
			D = client.getDirectoryEntry(dst);
		} catch (IOException e) {
			log.debug("Destination directory doesn't exists, will be created");
		}
		if(D != null && D.type != DirectoryEntryType.DIRECTORY) {
			log.error("Destination path points to a file");
			throw new IllegalArgumentException("Destination path points to a file. Please provide a directory");
		}
		return true;
	}
}
