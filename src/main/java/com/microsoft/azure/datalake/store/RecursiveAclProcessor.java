/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.acl.AclEntry;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recursively modify ACLs of a directory tree. This class spawns many threads and applies the ACLs in parallel.
 * Call the static method {@code processRequest} to do the work.
 *
 */
public class RecursiveAclProcessor {

    private enum PayloadType {
        PROCESS_DIRECTORY,
        MODIFY_ACL_FOR_SINGLE_ENTRY,
        SET_ACL_FOR_SINGLE_ENTRY,
        REMOVE_ACL_FOR_SINGLE_ENTRY,
    }

    private class Payload {
        public PayloadType type;
        public DirectoryEntry de;
        public Payload(PayloadType type, DirectoryEntry de) {
            this.type = type;
            this.de = de;
        }
    }


    private AdlAclOperation op;
    private final ProcessingQueue<Payload> queue = new ProcessingQueue<>();
    private ADLStoreClient client;
    private AtomicInteger opCountForProgressBar = new AtomicInteger(0);

    private static final int NUM_THREADS = 200;
    private static final int ENUMERATION_PAGESIZE = 16000;

    private String path;
    private List<AclEntry> aclSpec;

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
     * @param op {@link AdlAclOperation} enum value specifying the operation to perform
     * @return {@link RecursiveAclProcessorStats} object containing stats of the run
     * @throws IOException throws {@link IOException} if there is an error
     */
    public static RecursiveAclProcessorStats processRequest(ADLStoreClient client, String path, List<AclEntry> aclSpec, AdlAclOperation op) throws IOException {
        RecursiveAclProcessor p = new RecursiveAclProcessor();
        return p.processRequestInternal(client, path, aclSpec, op);
    }

    private RecursiveAclProcessorStats processRequestInternal(ADLStoreClient client, String path, List<AclEntry> aclSpec, AdlAclOperation op) throws IOException {
        this.client = client;
        this.path = path;
        this.aclSpec = aclSpec;
        this.op = op;
        DirectoryEntry de = client.getDirectoryEntry(path);
        if (de.type == DirectoryEntryType.FILE) {
            processFile(de);
        } else {
            processDirectory(de);
        }

        // Start threads in the processing thread-pool
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
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
                    if (payload.type == PayloadType.PROCESS_DIRECTORY) {
                        processDirectoryTree(payload.de.fullName);
                    } else if (payload.type == PayloadType.MODIFY_ACL_FOR_SINGLE_ENTRY) {
                        client.modifyAclEntries(payload.de.fullName, aclSpec);
                    } else if (payload.type == PayloadType.SET_ACL_FOR_SINGLE_ENTRY) {
                        client.setAcl(payload.de.fullName, aclSpec);
                    } else if (payload.type == PayloadType.REMOVE_ACL_FOR_SINGLE_ENTRY) {
                        client.removeAclEntries(payload.de.fullName, aclSpec);
                    }
                    queue.unregister();
                    if(opCountForProgressBar.incrementAndGet() % 1000 == 0) System.out.print('.');

                }
            } catch (IOException ex) {
                ex.printStackTrace();
                System.exit(4100);
            }
        }
    }

    private void processDirectoryTree(String directoryName) throws IOException {
        int pagesize = ENUMERATION_PAGESIZE;
        ArrayList<DirectoryEntry> list;
        boolean eol = false;
        String startAfter = null;

        do {
            list = (ArrayList<DirectoryEntry>) enumerateDirectoryInternal(directoryName, pagesize,
                    startAfter, null, null);
            if (list == null || list.size() == 0) break;
            for (DirectoryEntry de : list) {
                if (de.type == DirectoryEntryType.FILE) {
                    processFile(de);
                } else {
                    processDirectory(de);
                }
                startAfter = de.name;
            }
        } while (list.size() >= pagesize);
    }

    private void processDirectory(DirectoryEntry de) {
        queue.add(new Payload(PayloadType.PROCESS_DIRECTORY, de));        // queue the task to recurse this directory
        enqueueAclChange(de);
        directoryCount.incrementAndGet();
    }

    private void processFile(DirectoryEntry de) {
        enqueueAclChange(de);
        fileCount.incrementAndGet();
    }

    private void enqueueAclChange(DirectoryEntry de) {
        if (this.op == AdlAclOperation.modifyacl) {
            queue.add(new Payload(PayloadType.MODIFY_ACL_FOR_SINGLE_ENTRY, de)); // queue the task to setacl ACL on this directory
        } else if (this.op == AdlAclOperation.setacl) {
            queue.add(new Payload(PayloadType.SET_ACL_FOR_SINGLE_ENTRY, de)); // queue the task to setacl ACL on this directory
        } else if (this.op == AdlAclOperation.removeacl) {
            queue.add(new Payload(PayloadType.REMOVE_ACL_FOR_SINGLE_ENTRY, de)); // queue the task to setacl ACL on this directory
        }
    }

    private List<DirectoryEntry> enumerateDirectoryInternal(String path,
                                                            int maxEntriesToRetrieve,
                                                            String startAfter,
                                                            String endBefore,
                                                            UserGroupRepresentation oidOrUpn)
            throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialBackoffPolicy();
        OperationResponse resp = new OperationResponse();
        List<DirectoryEntry> dirEnt = Core.listStatus(path, startAfter, endBefore, maxEntriesToRetrieve, oidOrUpn,
                client, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error enumerating directory " + path);
        }
        return dirEnt;
    }
}
