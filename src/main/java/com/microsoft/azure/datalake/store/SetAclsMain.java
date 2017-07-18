/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.acl.AclEntry;
import com.microsoft.azure.datalake.store.oauth2.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Main method for the ACL operations
 */
class SetAclsMain
{

    public static void main(RequestedOperation op, ADLStoreClient client, String[] args )
    {
        if (args.length != 4 ) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            AdlsTool.usage(1000);
        }

        String path = null;
        List<AclEntry> acl = null;

        path = args[2];

        try {
            acl = AclEntry.parseAclSpec(args[3]);
        } catch (Exception ex) {
            System.out.println("Illegal ACLSpec specified: " + args[3]);
            AdlsTool.usage(1003);
        }

        if (acl != null) {
            try {
                long start = System.currentTimeMillis();
                RecursiveAclProcessorStats stats = RecursiveAclProcessor.processRequest(client, path, acl, op);
                long stop = System.currentTimeMillis();
                System.out.println("COMPLETE");
                System.out.println("# of Files Processed: " + stats.fileCount);
                System.out.println("# of Directories Processed: " + stats.directoryCount);
                System.out.println("Time taken: " + AdlsTool.timeString(stop-start));
            } catch (IOException ex) {
                System.out.println("Error setting ACLs");
                ex.printStackTrace();
                System.exit(5001);
            }
        }
    }


}
