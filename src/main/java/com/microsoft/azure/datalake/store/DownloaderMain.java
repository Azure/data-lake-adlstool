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
            Stats R = RemoteCopy.download(srcPath, dstPath, client, overwriteOption);
            long stop = System.currentTimeMillis();

            if(R.getSkippedTransfers().size() + R.getFailedTransfers().size() == 0) {
            	System.out.println("SUCCESSFULLY COMPLETE");
            } else {
            	System.out.println("DOWNLOAD FAILED FOR FEW FILES");
            }
            System.out.println("Time taken: " + AdlsTool.timeString(stop - start));
            System.out.println("# of Files Downloaded: " + R.getSuccessfulTransfers().size());
            System.out.println("Total number of Bytes downloaded: " + R.totalSizeInBytes);
            if(R.getSkippedTransfers().size() + R.getFailedTransfers().size() > 0) {
            	System.out.println("Failed downloads:");
                for(String file: R.getFailedTransfers()) {
                	System.out.println('\t' + file);
                }
                System.out.println("Files skipped");
                for(String file: R.getSkippedTransfers()) {
                	System.out.println('\t' + file);
                }
            }
        } catch (Exception ex) {
            System.out.println("Error downloading files");
            System.out.println(ex.getMessage());
            ex.printStackTrace();
            System.exit(5001);
        }
    }

}
