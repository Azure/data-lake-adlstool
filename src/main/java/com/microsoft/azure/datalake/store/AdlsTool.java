/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.datalake.store;


import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.RefreshTokenBasedTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.UserPasswordTokenProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * The class that contains the main method for the command-line tool.
 */
class AdlsTool {

    public static void main( String[] args ) {
        // Currently SetAcl is the only think this tool does
        // We can fill in more stuff in this method if the tool evolves to do more
        if (args.length < 1 ) {
            System.out.println("Illegal number of command-line parameters: " + args.length);
            usage(1000);
        }

        RequestedOperation op = null;
        try {
            op = RequestedOperation.valueOf(args[0].toLowerCase());
        } catch (IllegalArgumentException ex) {
            System.out.println("Illegal operation specified: " + args[0]);
            AdlsTool.usage(1001);
        }
        // do not support setacl for now on the command-line. Setacl, if used incorrectly, can lead to really
        // bad ACL state. However the code has support, so people who know what they are doing could use the
        // RecursiveAclProcessor.ProcessRequest(...) method to write their own tool (or comment out this check and
        // recompile this tool)
        if (op == RequestedOperation.setacl) {
            System.out.println("Illegal operation specified (setacl)");
            AdlsTool.usage(1011);
        }

        ADLStoreClient client = null;
        try {
            client = getClient(readPropertiesFile(args[1]));
            String toolVersion = AdlsTool.class.getPackage().getImplementationVersion();
            if (toolVersion == null) toolVersion = "UnknownVersion";
            ADLStoreOptions options = new ADLStoreOptions().setUserAgentSuffix("AdlsTool-"+ toolVersion + "/" + op.toString());
            client.setOptions(options);
        } catch (FileNotFoundException ex) {
            System.out.println("Credential file not found: " + args[1]);
            AdlsTool.usage(2011);
        } catch (Exception ex) {
            System.out.println("Error reading account credentials: " + ex.toString());
            AdlsTool.usage(2012);
        }

        switch (op) {
            case modifyacl:
            case removeacl:
                SetAclsMain.main(op, client, args);
                break;
            case upload:
                UploaderMain.main(op, client, args);
                break;
            default:
                System.out.println("Illegal operation specified (setacl)");
                AdlsTool.usage(1011);
        }

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
            AdlsTool.usage(2100);
        }

        String account = prop.getProperty("account").toLowerCase();
        return ADLStoreClient.createClient(account, tokenProvider);
    }

    private static Properties readPropertiesFile(String filename) throws IOException {
        File f = new File(filename);
        FileInputStream instr = new FileInputStream(filename);
        Properties prop = new Properties();
        prop.load(instr);
        return prop;
    }

    public static String timeString(long millis) {
        long seconds = millis / 1000;
        long minutes = seconds / 60;
        long displayseconds = seconds % 60;
        if (minutes > 0) {
            return String.format("%dm : %ds", minutes, displayseconds);
        } else {
            return String.format("%d seconds", displayseconds);
        }
    }
    
    public static int threadSetup() {
        // Determine the number of threads to use
        int numThreads = Runtime.getRuntime().availableProcessors() * 10; // heuristic: 10 times number of processors
        Properties p = System.getProperties();
        String threadStr = p.getProperty("adlstool.threads");
        if (threadStr != null) {
            try {
                numThreads = Integer.parseInt(threadStr);
            } catch (NumberFormatException ex) {
                System.out.println("Illegal threadcount in system property adlstool.threads : " + threadStr);
                System.exit(1008);
            }
        }
        System.setProperty("http.keepAlive", "true");
        System.setProperty("http.maxConnections", (new Integer(numThreads)).toString());
        if(System.getProperty("java.runtime.version").startsWith("1.8.")) {
        	System.setProperty("https.cipherSuites", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        }
    	return numThreads;
    }

    public static void usage(int exitCode) {
        System.out.println();
        System.out.println("ADLS Java command-line tool");
        System.out.println("Usage:");
        System.out.println("  adlstool <modifyacl|removeacl> <credfile> <path> \"<aclspec>\"");
        System.out.println("  adlstool upload <credfile> <sourcePath> <destinationPath>");
        System.out.println();
        System.out.println("For upload:");
        System.out.println("  sourcePath= local path to a file or directory to be uploaded");
        System.out.println("  destinationPath= path to a directory on ADLS to upload the file/directory to");
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
