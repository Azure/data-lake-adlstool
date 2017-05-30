/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 *
 */

package com.microsoft.azure.datalake.store;


/**
 * The class that contains the main method for the command-line tool.
 */
class AdlsTool {

    public static void main( String[] args ) {
        // Currently SetAcl is the only think this tool does
        // We can fill in more stuff in this method if the tool evolves to do more
        SetAcls.main(args);
    }
}
