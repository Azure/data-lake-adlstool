package com.microsoft.azure.datalake.store;


import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;


public class FileUploader {
	private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.FileUploader");
	private static int threadCount;
	private ProcessingQueue<MetaData> metaDataQ;
	private ConsumerQueue<Job> jobQ;
	private ADLStoreClient client;
	private Thread[] executorThreads;
	private JobExecutor[] executor;
	private IfExists overwrite;
	private EnumerateFile jobGen;
	
	public FileUploader(ADLStoreClient client, IfExists overwriteOption) {
		metaDataQ = new ProcessingQueue<MetaData>();
		jobQ = new ConsumerQueue<Job>(new PriorityQueue<Job>());
		threadCount = AdlsTool.threadSetup();
		this.client = client;
		this.overwrite = overwriteOption;
	}
	
	/*
	 * Uploads the given source dir/file to a directory on ADLS.
	 * @param source Local Source directory or file to upload from.
	 * @param destination Destination directory to copy the files to.
	 * @param client ADLStoreClient to use to upload the file.
	 */
	public static Stats upload(String source, String destination, ADLStoreClient client, IfExists overwriteOption) throws IOException, InterruptedException {
		FileUploader F = new FileUploader(client, overwriteOption);
		return F.uploadInternal(source, destination);
	}
	
	public static Stats download(String source, String destination, ADLStoreClient client, IfExists overwriteOption) {
		FileUploader F = new FileUploader(client, overwriteOption);
		DirectoryEntry entry = null;
		Stats R = new Stats();
		
		try {
			entry = client.getDirectoryEntry(source);
		} catch (IOException e) {
			log.error("Error collecting details of source from ADLS");
			log.error(e.getMessage());
			System.out.println("Unable to collect details of source: " + source + " from ADLS");
			R.failedTransfers.add(source);
			return R;
		}
		try {
			R = F.download(entry, destination);
		} catch (IOException | InterruptedException e) {
			log.error(e.getMessage());
		}
		return R;
	}
	
	private Stats uploadInternal(String source, String destination) throws InterruptedException, IOException {
		if(source == null) {
			throw new IllegalArgumentException("source is null");
		} else if(destination == null) {
			throw new IllegalArgumentException("destination is null");
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
		
		if(!isDirectory(srcDir)) {
			if(!verifyDestination(destination)) {
				return new Stats();
			}
		}
		
		return upload(srcDir, destination);
	}
	
	private static boolean isDirectory(File inFile) {
		return inFile.listFiles() != null;
	}
	
	private void startUploaderThreads(ConsumerQueue<Job> jobQ) {
		executorThreads = new Thread[threadCount];
		executor = new JobExecutor[threadCount];
		for(int i = 0; i < executorThreads.length; i++) {
			executor[i] = new JobExecutor(jobQ, client, overwrite);
			executorThreads[i] = new Thread(executor[i]);
			executorThreads[i].start();
		}
	}
	
	private Thread startEnumeration(File source, String destination) {
		jobGen = new EnumerateFile(source, destination, metaDataQ, jobQ);
		Thread t = new Thread(jobGen);
		t.start();
		return t;
	}
	
	private Thread startEnumeration(DirectoryEntry source, String destination) {
		jobGen = new EnumerateFile(source, destination, metaDataQ, jobQ, client);
		Thread t = new Thread(jobGen);
		t.start();
		return t;
	}
	
	private Stats download(DirectoryEntry source, String destination) throws IOException, InterruptedException {
		Thread generateJob = startEnumeration(source, destination);
		startUploaderThreads(jobQ);
		Thread statusThread = waitForCompletion(generateJob);
		Stats R = joinUploaderThreads();
		statusThread.interrupt();
		return R;
	}
	
	private Stats upload(File source, String destination) throws IOException, InterruptedException {
		Thread generateJob = startEnumeration(source, destination);
		startUploaderThreads(jobQ);
		Thread statusThread = waitForCompletion(generateJob);
		Stats R = joinUploaderThreads();
		statusThread.interrupt();
		return R;
	}
	
	private Thread waitForCompletion(Thread generateJob) throws InterruptedException {
		generateJob.join();
		jobQ.markComplete(); // Consumer threads wait until enumeration is active.
		StatusBar statusBar = new StatusBar(jobGen.getBytesToTransmit(), executor);
		Thread status = new Thread(statusBar);  // start a status bar.
		status.start();
		return status;
	}
	
	private Stats joinUploaderThreads() throws InterruptedException {
		Stats result = new Stats();
		for(int i = 0; i < executorThreads.length; i++) {
			executorThreads[i].join();
			result.update(executor[i].stats);
		}
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
	
	class StatusBar implements Runnable {
		private long totalBytesToTransfer;
		private JobExecutor[] executors;
		private static final long sleepTime = 500;
		StatusBar(long bytesToTransfer, JobExecutor[] uploaders) {
			totalBytesToTransfer = bytesToTransfer;
			this.executors = uploaders;
		}
		public void run() {
			int percent = 0;
			while(percent < 100) {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					break;
				}
				long bytesTransferred = 0;
				for(int i = 0; i < executors.length; i++) {
					bytesTransferred += executor[i].stats.getBytesTransferred();
				}
				percent = (int) ((100.0*bytesTransferred)/totalBytesToTransfer);
				System.out.printf("%% Complete: %d\r", percent);
			}
		}
		
	}
}
