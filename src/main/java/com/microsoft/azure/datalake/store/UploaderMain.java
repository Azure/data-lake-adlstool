package com.microsoft.azure.datalake.store;

import java.io.IOException;

public class UploaderMain {
	static final int numberOfArguments = 4;
	
	
    public static void main(RequestedOperation op, ADLStoreClient client, String[] args ) {
        if (args.length < numberOfArguments) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            AdlsTool.usage(1000);
        }

        String srcPath = args[2].trim();
        String dstPath = args[3].trim();
        
        if (srcPath == null || srcPath.length() == 0) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            AdlsTool.usage(1200);
        }

        if (dstPath == null || dstPath.length() == 0) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            AdlsTool.usage(1201);
        }

        try {
            long start = System.currentTimeMillis();
            UploadResult R = FileUploader.upload(srcPath, dstPath, client);
            long stop = System.currentTimeMillis();
            AdlsTool.resetCipher();
            if(R.success) {
            	System.out.println("SUCCESSFULLY COMPLETE");
            } else {
            	System.out.println("UPLOAD FAILED");
            }
            System.out.println("# of Files Uploaded: " + R.successfulUploads.size());
            System.out.println("# of Failed Uploads: " + R.failedUplaods.size());
            System.out.println("Total number of Bytes uploaded " + R.totalSizeInBytes);
            System.out.println("Time taken: " + AdlsTool.timeString(stop - start));
        } catch (Exception ex) {
            System.out.println("Error uploading files");
            ex.printStackTrace();
            System.exit(5001);
        }
    }
}
