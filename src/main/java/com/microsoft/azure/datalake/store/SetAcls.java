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
class SetAcls
{

    public static void main( String[] args )
    {
        if (args.length != 4 ) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            usage(1000);
        }

        String path = null;
        AdlAclOperation op = null;
        List<AclEntry> acl = null;
        ADLStoreClient client = null;

        try {
            op = AdlAclOperation.valueOf(args[0].toLowerCase());
        } catch (IllegalArgumentException ex) {
            System.out.println("Illegal operation specified: " + args[0]);
            usage(1001);
        }

        // do not support setacl for now on the command-line. Setacl, if used incorrectly, can lead to really
        // bad ACL state. However the code has support, so people who know what they are doing could use the
        // RecursiveAclProcessor.ProcessRequest(...) method to write their own tool (or comment out this check and
        // recompile this tool)
        if (op == AdlAclOperation.setacl) {
            System.out.println("Illegal operation specified (setacl)");
            usage(1011);
        }

        try {
            client = getClient(readPropertiesFile(args[1]));
            String toolVersion = AdlsTool.class.getPackage().getImplementationVersion();
            if (toolVersion == null) toolVersion = "UnknownVersion";
            ADLStoreOptions options = new ADLStoreOptions().setUserAgentSuffix("AdlsTool-"+ toolVersion + "/" + op.toString());
            client.setOptions(options);
        } catch (FileNotFoundException ex) {
            System.out.println("Credential file not found: " + args[1]);
            usage(2011);
        } catch (Exception ex) {
            System.out.println("Error reading account credentials: " + ex.toString());
            usage(2012);
        }

        path = args[2];

        try {
            acl = AclEntry.parseAclSpec(args[3]);
        } catch (Exception ex) {
            System.out.println("Illegal ACLSpec specified: " + args[3]);
            usage(1003);
        }

        if (acl != null) {
            try {
                long start = System.currentTimeMillis();
                RecursiveAclProcessorStats stats = RecursiveAclProcessor.processRequest(client, path, acl, op);
                long stop = System.currentTimeMillis();
                System.out.println("COMPLETE");
                System.out.println("# of Files Processed: " + stats.fileCount);
                System.out.println("# of Directories Processed: " + stats.directoryCount);
                System.out.println("Time taken: " + timeString(stop-start));
            } catch (IOException ex) {
                System.out.println("Error setting ACLs");
                ex.printStackTrace();
                System.exit(5001);
            }
        }
    }

    private static Properties readPropertiesFile(String filename) throws IOException {
        File f = new File(filename);
        FileInputStream instr = new FileInputStream(filename);
        Properties prop = new Properties();
        prop.load(instr);
        return prop;
    }

    private static ADLStoreClient getClient(Properties prop) {

        AccessTokenProvider tokenProvider = null;
        String credtype = prop.getProperty("credtype").toLowerCase();

        if ("clientcredentials".equals(credtype.toLowerCase())) {
            tokenProvider = new ClientCredsTokenProvider(prop.getProperty("authurl"),
                                                         prop.getProperty("clientid"),
                                                         prop.getProperty("credential") );
        } else if ("refreshtoken".equals(credtype.toLowerCase())) {
            tokenProvider = new RefreshTokenBasedTokenProvider(
                    prop.getProperty("clientid"),
                    prop.getProperty("refreshtoken") );
        } else if ("userpassword".equals(credtype.toLowerCase())) {
            tokenProvider = new UserPasswordTokenProvider(
                    prop.getProperty("clientid"),
                    prop.getProperty("username") ,
                    prop.getProperty("password") );
        } else {
            System.out.println("Unknown Token provider type " + prop.getProperty("type"));
            usage(2100);
        }

        String account = prop.getProperty("account").toLowerCase();
        return ADLStoreClient.createClient(account, tokenProvider);
    }

    private static String timeString(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long displayseconds = seconds % 60;
        if (minutes > 0) {
            return String.format("%dm : %ds", minutes, displayseconds);
        } else {
            return String.format("%d seconds", displayseconds);
        }
    }

    private static void usage(int exitCode) {
        System.out.println();
        System.out.println("ADLS Java command-line tool");
        System.out.println("Usage:");
        System.out.println("  adlstool <modifyacl|removeacl> <credfile> <path> \"<aclspec>\"");
        System.out.println();
        System.out.println();
        System.out.println("Where <credfile> is the path to a java property file that contains the following properties:");
        System.out.println("  account= fully qualified domain name of the Azure Data Lake Store account");
        System.out.println("  credtype= the type of credential; one of clientcredentials or refreshtoken");
        System.out.println();
        System.out.println("For clientcredentials, provide these three values:");
        System.out.println("  authurl= the OAuth2 endpoint from AAD");
        System.out.println("  clientid= the application ID");
        System.out.println("  credential= the (secret) key");
        System.out.println();
        System.out.println("For refreshtoken, provide these two values:");
        System.out.println("  clientid= the ID of the user");
        System.out.println("  refreshtoken= the refresh token");
        System.out.println();
        System.exit(exitCode);
    }
}
