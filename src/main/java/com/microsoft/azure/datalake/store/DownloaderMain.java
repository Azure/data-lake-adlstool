package com.microsoft.azure.datalake.store;

public class DownloaderMain {

	public static void main(RequestedOperation op, ADLStoreClient client, String[] args ) {
        int numberOfArguments = 4;
		if (args.length < numberOfArguments ) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            AdlsTool.usage(1000);
        }

        String srcPath = args[2].trim();
        String dstPath = args[3].trim();
        String overwrite = args.length == numberOfArguments ? null : args[4].trim();
        if (srcPath == null || srcPath.length() == 0) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            AdlsTool.usage(1200);
        }

        if (dstPath == null || dstPath.length() == 0) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            AdlsTool.usage(1201);
        }
        
        if(overwrite != null && !overwrite.equals("overwrite")) {
        	System.out.println("Illegal optional parameter to overwrite");
        	AdlsTool.usage(1201);
        }
        IfExists overwriteOption = overwrite != null ? IfExists.OVERWRITE : IfExists.FAIL; 
        try {
            long start = System.currentTimeMillis();
            UploadResult R = FileUploader.download(srcPath, dstPath, client, overwriteOption);
            long stop = System.currentTimeMillis();

            if(R.getSkippedUploads().size() + R.getFailedUploads().size() == 0) {
            	System.out.println("SUCCESSFULLY COMPLETE");
            } else {
            	System.out.println("UPLOAD FAILED FOR FEW FILES");
            }
            System.out.println("Time taken: " + AdlsTool.timeString(stop - start));
            System.out.println("# of Files Uploaded: " + R.getSuccessfulUploads().size());
            System.out.println("Total number of Bytes uploaded: " + R.totalSizeInBytes);
            if(R.getSkippedUploads().size() + R.getFailedUploads().size() > 0) {
            	System.out.println("Files skipped or failed upload:");
                for(String file: R.getFailedUploads()) {
                	System.out.println('\t' + file);
                }
                for(String file: R.getSkippedUploads()) {
                	System.out.println('\t' + file);
                }
            }
        } catch (Exception ex) {
            System.out.println("Error uploading files");
            System.out.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(5001);
        }
    }

}
