/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.util.expimp;

/**
 * Implementation class for ObjectStoreFactory
 */
class ObjectStoreFactoryImpl extends ObjectStoreFactory {

    @Override
    AbstractStoreExport
        createObjectStoreExport(String storeName,
                                String[] helperHosts,
                                String userName,
                                String securityFile,
                                String containerName,
                                String serviceName,
                                String objectStoreUserName,
                                String objectStorePassword,
                                String serviceUrl,
                                boolean json) {

        return new ObjectStoreExport(storeName,
                                     helperHosts,
                                     userName,
                                     securityFile,
                                     containerName,
                                     serviceName,
                                     objectStoreUserName,
                                     objectStorePassword,
                                     serviceUrl,
                                     json);
    }

    @Override
    AbstractStoreImport
        createObjectStoreImport(String storeName,
                                String[] helperHosts,
                                String userName,
                                String securityFile,
                                String containerName,
                                String serviceName,
                                String objectStoreUserName,
                                String objectStorePassword,
                                String serviceUrl,
                                String status,
                                boolean json) {

        return new ObjectStoreImport(storeName,
                                     helperHosts,
                                     userName,
                                     securityFile,
                                     containerName,
                                     serviceName,
                                     objectStoreUserName,
                                     objectStorePassword,
                                     serviceUrl,
                                     status,
                                     json);
    }
}
