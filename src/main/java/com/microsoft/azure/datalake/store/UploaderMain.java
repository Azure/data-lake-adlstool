package com.microsoft.azure.datalake.store;

import java.io.IOException;

public class UploaderMain {

    public static void main(RequestedOperation op, ADLStoreClient client, String[] args ) {
        if (args.length != 4 ) {
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
            //RecursiveAclProcessorStats stats = RecursiveAclProcessor.processRequest(client, path, acl, op);
            long stop = System.currentTimeMillis();
            System.out.println("COMPLETE");
            //System.out.println("# of Files Processed: " + stats.fileCount);
            //System.out.println("# of Directories Processed: " + stats.directoryCount);
            System.out.println("Time taken: " + AdlsTool.timeString(stop - start));
        } catch (Exception ex) {
            System.out.println("Error uploading files");
            ex.printStackTrace();
            System.exit(5001);
        }
    }
}
