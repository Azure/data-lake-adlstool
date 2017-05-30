package com.microsoft.azure.datalake.store;

/**
 * Provides stats of the file run.
 */
public class RecursiveAclProcessorStats {
    /**
     * number of files processed
     */
    public final long fileCount;
    /**
     * number of directories processed
     */
    public final long directoryCount;

    RecursiveAclProcessorStats(long files, long dirs) {
        this.fileCount = files;
        this.directoryCount = dirs;
    }
}
