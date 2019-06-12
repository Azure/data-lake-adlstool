/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.acl.AclEntry;
import com.microsoft.azure.datalake.store.acl.AclScope;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recursively modify ACLs of a directory tree. This class spawns many threads and applies the ACLs in parallel.
 * Call the static method {@code processRequest} to do the work.
 *
 */
public class RecursiveAclProcessor {

    private enum PayloadType {
        MODIFY_ACL_FOR_SINGLE_ENTRY,
        SET_ACL_FOR_SINGLE_ENTRY,
        REMOVE_ACL_FOR_SINGLE_ENTRY,
        PROCESS_DIRECTORY,
    }

    private class Payload implements Comparable<Payload> {
        public PayloadType type;
        public DirectoryEntry de;
        public String continuation;
        public Payload(PayloadType type, DirectoryEntry de) {
            this.type = type;
            this.de = de;
            this.continuation = "";
        }

        public Payload(PayloadType type, DirectoryEntry de, String continuation) {
            this.type = type;
            this.de = de;
            this.continuation = continuation;
        }

        @Override
        public int compareTo(Payload that) {
            return this.type.ordinal() - that.type.ordinal();
        }
    }


    private RequestedOperation op;
    private final ProcessingQueue2<Payload> queue = new ProcessingQueue2<>();
    private ADLStoreClient client;
    private AtomicInteger opCountForProgressBar = new AtomicInteger(0);

    private static final int ENUMERATION_PAGESIZE = 16000;

    private List<AclEntry> aclSpec;
    private List<AclEntry> aclSpecForFiles = new ArrayList<>(10);

    // for stats
    private AtomicLong fileCount = new AtomicLong(0);
    private AtomicLong directoryCount = new AtomicLong(0);

    // private constructor, to hide visibility
    private RecursiveAclProcessor() {
    }

    /**
     * Add ACLs recursively to a directory tree.
     *
     * @param client {@code ADLStoreClient} object to use
     * @param path the root of the path to set ACLs for
     * @param aclSpec the ACL list to apply
     * @param op {@link RequestedOperation} enum value specifying the operation to perform
     * @return {@link RecursiveAclProcessorStats} object containing stats of the run
     * @throws IOException throws {@link IOException} if there is an error
     */
    public static RecursiveAclProcessorStats processRequest(ADLStoreClient client, String path, List<AclEntry> aclSpec, RequestedOperation op) throws IOException {
        RecursiveAclProcessor p = new RecursiveAclProcessor();
        return p.processRequestInternal(client, path, aclSpec, op);
    }

    private RecursiveAclProcessorStats processRequestInternal(ADLStoreClient client, String path, List<AclEntry> aclSpec, RequestedOperation op) throws IOException {
        this.client = client;
        this.aclSpec = aclSpec;
        this.op = op;
        for (AclEntry e : aclSpec) {
            if (e.scope == AclScope.ACCESS) {
                this.aclSpecForFiles.add(e);
            }
        }

        DirectoryEntry de = client.getDirectoryEntry(path);
        if (de.type == DirectoryEntryType.FILE) {
            processFile(de);
        } else {
            processDirectory(de, "");
        }

        // Determine the number of threads to use
        int numThreads = AdlsTool.threadSetup();

        // Start threads in the processing thread-pool
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new RecursiveAclProcessor.ThreadProcessor());
            threads[i].start();
        }

        // wait for all threads to get done
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        return new RecursiveAclProcessorStats(fileCount.get(), directoryCount.get());
    }




    private class ThreadProcessor  implements Runnable {

        public void run() {
            try {
                Payload payload;
                while ((payload = queue.poll()) != null) {
                    try {
                        if (payload.type == PayloadType.PROCESS_DIRECTORY) {
                            processDirectoryTree(payload.de, payload.continuation);
                        } else if (payload.type == PayloadType.MODIFY_ACL_FOR_SINGLE_ENTRY) {
                            if (payload.de.type == DirectoryEntryType.FILE) {
                                client.modifyAclEntries(payload.de.fullName, aclSpecForFiles);
                            } else {
                                client.modifyAclEntries(payload.de.fullName, aclSpec);
                            }
                        } else if (payload.type == PayloadType.SET_ACL_FOR_SINGLE_ENTRY) {
                            if (payload.de.type == DirectoryEntryType.FILE) {
                                client.setAcl(payload.de.fullName, aclSpecForFiles);
                            } else {
                                client.setAcl(payload.de.fullName, aclSpec);
                            }
                        } else if (payload.type == PayloadType.REMOVE_ACL_FOR_SINGLE_ENTRY) {
                            if (payload.de.type == DirectoryEntryType.FILE) {
                                client.removeAclEntries(payload.de.fullName, aclSpecForFiles);
                            } else {
                                client.removeAclEntries(payload.de.fullName, aclSpec);
                            }
                        }
                    } catch (ADLException ex) {
                        if (ex.httpResponseCode == 404) {
                            // swallow - the file or directory got deleted after we enumerated it
                        } else {
                            throw ex;
                        }
                    } finally {
                        queue.unregister();
                        if (opCountForProgressBar.incrementAndGet() % 1000 == 0) System.out.print('.');
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
                System.exit(4100);
            }
        }
    }

    private void processDirectoryTree(DirectoryEntry directoryEntry, String continuationToken) throws IOException {
        List<DirectoryEntry> entries;
        String directoryName = directoryEntry.fullName;

        DirectoryEntryListWithContinuationToken dirEntContToken =  enumerateDirectoryInternal(directoryName, ENUMERATION_PAGESIZE,
                continuationToken, null, null);
        entries = dirEntContToken.getEntries();
        if (entries == null || entries.isEmpty())
            return ;
        for (DirectoryEntry de : entries) {
            if (de.type == DirectoryEntryType.FILE) {
                processFile(de);
            } else {
                processDirectory(de, "");
            }

        }
        continuationToken = dirEntContToken.getContinuationToken();
        if(continuationToken != null && !continuationToken.isEmpty())
            processDirectory(directoryEntry, continuationToken);
    }

    private void processDirectory(DirectoryEntry de, String continuationToken) {
        queue.add(new Payload(PayloadType.PROCESS_DIRECTORY, de, continuationToken));        // queue the task to recurse this directory
        enqueueAclChange(de);
        if (continuationToken == null || continuationToken.isEmpty()) // Means the first time called on this directory
            directoryCount.incrementAndGet();
    }

    private void processFile(DirectoryEntry de) {
        if (aclSpecForFiles.size() != 0) enqueueAclChange(de);
        fileCount.incrementAndGet();
    }

    private void enqueueAclChange(DirectoryEntry de) {
        if (this.op == RequestedOperation.modifyacl) {
            queue.add(new Payload(PayloadType.MODIFY_ACL_FOR_SINGLE_ENTRY, de)); // queue the task to setacl ACL on this directory
        } else if (this.op == RequestedOperation.setacl) {
            queue.add(new Payload(PayloadType.SET_ACL_FOR_SINGLE_ENTRY, de)); // queue the task to setacl ACL on this directory
        } else if (this.op == RequestedOperation.removeacl) {
            queue.add(new Payload(PayloadType.REMOVE_ACL_FOR_SINGLE_ENTRY, de)); // queue the task to setacl ACL on this directory
        }
    }

    private DirectoryEntryListWithContinuationToken enumerateDirectoryInternal(String path,
                                                            int maxEntriesToRetrieve,
                                                            String startAfter,
                                                            String endBefore,
                                                            UserGroupRepresentation oidOrUpn)
            throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialBackoffPolicy();
        opts.timeout = 2 * client.timeout;
        OperationResponse resp = new OperationResponse();
        DirectoryEntryListWithContinuationToken dirEnt  = Core.listStatusWithToken(path, startAfter, endBefore, maxEntriesToRetrieve, oidOrUpn, client, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error enumerating directory " + path);
        }
        return dirEnt;
    }
}
