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
 * Factory class used to create instances of ObjectStoreExport and
 * ObjectStoreImport using reflection.
 */
abstract class ObjectStoreFactory {

    static final String factoryClassName =
        "oracle.kv.util.expimp.ObjectStoreFactoryImpl";
    static final String[] cloudStorageClassNames =
        {/* class in oracle.cloud.storage.api.jar */
         "oracle.cloud.storage.CloudStorage",
         /* class in jersey-client.jar */
         "com.sun.jersey.api.client.ClientHandlerException",
         /* class in jersey-core.jar */
         "com.sun.jersey.spi.inject.Errors$Closure",
         /* class in jettison.jar */
         "org.codehaus.jettison.json.JSONException"};

    /**
     * Creates and returns an ObjectStoreFactory if the underlying Oracle
     * storage cloud service class is available, otherwise returns null
     *
     * @return the factory or null
     */
    static ObjectStoreFactory createFactory() {

        /*
         * Check if oracle storage cloud service dependent classes are present.
         */
        for (String className : cloudStorageClassNames) {
            try {
                Class.forName(className);
            } catch (ClassNotFoundException e) {
                return null;
            }
        }

        try {
            Class<?> factoryClass = Class.forName(factoryClassName);
            return (ObjectStoreFactory) factoryClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception creating" +
                                       " ObjectStoreFactory: " + e.getMessage(),
                                       e);
        }
    }

    abstract AbstractStoreExport createObjectStoreExport(
        String storeName,
        String[] helperHosts,
        String userName,
        String securityFile,
        String containerName,
        String serviceName,
        String objectStoreUserName,
        String objectStorePassword,
        String serviceUrl,
        boolean json);

    abstract AbstractStoreImport createObjectStoreImport(
        String storeName,
        String[] helperHosts,
        String userName,
        String securityFile,
        String containerName,
        String serviceName,
        String objectStoreUserName,
        String objectStorePassword,
        String serviceUrl,
        String status,
        boolean json);
}
