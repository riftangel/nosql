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

package security;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

/**
 * A complete example to show how to use Kerberos to authenticate to Oracle
 * NoSQL Database.
 * <h4>Preparation</h4>
 * <ul>
 *
 * <li> Download and install Kerberos software for all the server nodes and KDC
 * nodes.
 *
 * <li> Set up the Kerberos configuration. Start up Kerberos key distribution
 * center.
 *
 * <p>
 * See <a href="http://web.mit.edu/kerberos/krb5-latest/doc/index.html">
 * MIT Kerberos documentation</a>.
 * </ul>
 * <h4>Server setup</h4>
 * <ul>
 * <li> Build an administrative principal for Oracle NoSQL Database to create
 * the service principals. "admin/admin@EXAMPLE.COM" will be used for this
 * example.  Note that the Kerberos ACL file needs to provide administrative
 * privileges for the admin principal.
 * <pre>
 * kadmin.local:  addprinc admin/admin
 * Enter password for principal "admin/admin@EXAMPLE.COM":
 * Re-enter password for principal "admin/admin@EXAMPLE.COM":
 * </pre>
 * Add full privileges for the admin principal to the Kerberos ACL file:
 * <pre>
 * admin/admin@EXAMPLE.COM  *
 * </pre>
 * <li> Build a keytab file for the administrative principal:
 * <pre>
 * kadmin.local:  ktadd -k /tmp/admin.keytab admin/admin
 * </pre>
 * Copy the keytab file to each node in the same location. We save it in "/tmp"
 * for this example.
 *
 * <li> Deploy the Kerberos-based store according to the
 * <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSSEC-GUID-5F3DC029-4B6C-44D7-A8C6-358F16FFD036">
 * Performing a Secure Oracle NoSQL Database Installation with Kerberos</a>.
 *
 * <p>
 * We will set up a one node store using "localhost:5000" and the service
 * principal name "oraclenosql/localhost@EXAMPLE.COM" for this example.
 *
 * <p>
 * The following arguments need to be specified when running "makebootconfig":
 *
 * <pre>
 * -admin-principal admin/admin
 * -kadmin-keytab /tmp/admin.keytab
 * </pre>
 *
 * </ul>
 * <h4>Client setup</h4>
 * <ul>
 * <li> Create the principal for client user.  User "krbuser" will be used for
 * this example:
 *
 * <pre>
 * kadmin:  addprinc krbuser
 * Enter password for principal "krbuser@EXAMPLE.COM":
 * Re-enter password for principal "krbuser@EXAMPLE.COM":
 * </pre>
 *
 * <li> Create the keytab file for client user:
 *
 * <pre>
 * kadmin:  ktadd -k /tmp/krbuser.keytab krbuser
 * </pre>
 *
 * <p>
 * We save the client keytab file in "/tmp/krbuser.keytab" for this example.
 *
 * <li> Login to the store as the administrative user, see
 * <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSSEC-GUID-BBDFC82A-AD51-4AA0-A470-9416D926E58B">
 * Performing a Secure Oracle NoSQL Database Installation</a>.
 *
 * <p>
 * Register the external user name with Oracle NoSQL Database using the same
 * name as the full principal name of user's Kerberos principal:
 *
 * <pre>
 * execute 'CREATE USER "krbuser@EXAMPLE.COM" IDENTIFIED EXTERNALLY'
 * </pre>
 * </ul>
 * <h3>Login to Oracle NoSQL Database</h3>
 * <p>
 * In this example, we provide two different ways to login to Oracle NoSQL
 * Database.
 *
 * <p>
 * The first way to perform a Kerberos login uses the Java Authentication and
 * Authorization Service (JAAS) login API.
 *
 * <ul>
 * <li> Create a JAAS configuration file, and use the JAAS API to get the
 * credentials from the KDC. See
 * <a href="https://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/tutorials/GeneralAcnOnly.html">
 * JAAS Authentication Tutorial</a>.
 *
 * <p>
 * An example JAAS configuration file "jaas.config" is provided in this
 * package.  Please make sure the path of keytab file is set to the correct
 * place for the example.
 *
 * <li> Create a client security configuration file and fill in the properties
 * required for the SSL connection:
 * <ul>
 * <li>oracle.kv.transport=ssl
 * <li>oracle.kv.ssl.trustStore=/tmp/store.trust
 * </ul>
 * In this example, we assume that the store's trust file (public key
 * information) has been copied from the security root directory to
 * "/tmp/store.trust" for use by the client.  See
 * <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSSEC-GUID-11EC69FE-2C29-4237-8A23-9099EAB47B4E">
 * Encryption</a>.
 * <li> Fill in the security properties for Kerberos authentication:
 * <ul>
 * <li>oracle.kv.auth.username=krbuser
 * <li>oracle.kv.auth.external.mechanism=kerberos
 * <li>oracle.kv.auth.kerberos.services=localhost:oraclenosql/localhost@EXAMPLE.COM
 * </ul>
 * <li> The application should perform a JAAS login and then obtain the
 * store from within a call to <code>Subject.doAs</code>. Oracle NoSQL Database
 * will obtain the credentials from the global access controller directly for
 * the login. Specify the "-useJAAS" option on the command line in order to use
 * JAAS when running this example.
 *
 * <p>
 * The path of the JAAS configuration file needs to be set using the
 * "java.security.auth.login.config" system property.
 *
 * <p>
 * The command to run the example using JAAS is:
 *
 * <pre>
 * java -Djava.security.auth.login.config=$SECURITY_EXAMPLE_PATH/jaas.config \
 *     -cp $KVHOME/lib/kvstore.jar:. security.KerberosAuthenticationExample \
 *     -store kvstore -host localhost -port 5000 -useJAAS
 * </pre>
 * </ul>
 * The second way to perform a Kerberos login is to provide additional Kerberos
 * authentication information in the security login properties.
 * <ul>
 * <li> One of the following two parameters should be added to the security
 * login properties to specify how to obtain Kerberos credentials:
 * <pre>
 * oracle.kv.auth.kerberos.ccache
 * oracle.kv.auth.kerberos.keytab
 * </pre>
 * See
 * <a href="https://www.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/18.1&id=NSSEC-GUID-014966CB-B4DA-4DC5-AD25-5704294CEA07">
 * Using Security Properties to Login</a>.
 * In this example, we use the client keytab file created in
 * "/tmp/krbuser.keytab" to get the Kerberos credentials.
 *
 * <li> When obtaining the KVStore instance, the KVStoreFactory will try to get
 * the credentials for the user automatically.
 *
 * <p>
 * The command to run the example using security properties is:
 *
 * <pre>
 * java -cp $KVHOME/lib/kvstore.jar:. security.KerberosAuthenticationExample \
 *     -store kvstore -host localhost -port 5000
 * </pre>
 *
 * </ul>
 */
public class KerberosAuthenticationExample {

    KVStoreConfig kvConfig;
    boolean useJAAS = false;

    public static void main(String[] args) throws Exception {
        KerberosAuthenticationExample example =
            new KerberosAuthenticationExample(args);
        example.runExample();
    }

    KerberosAuthenticationExample(String[] argv) {

        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";
        final int nArgs = argv.length;
        int argc = 0;

        while (argc < nArgs) {
            final String thisArg = argv[argc++];

            if (thisArg.equals("-store")) {
                if (argc < nArgs) {
                    storeName = argv[argc++];
                } else {
                    usage("-store requires an argument");
                }
            } else if (thisArg.equals("-host")) {
                if (argc < nArgs) {
                    hostName = argv[argc++];
                } else {
                    usage("-host requires an argument");
                }
            } else if (thisArg.equals("-port")) {
                if (argc < nArgs) {
                    hostPort = argv[argc++];
                } else {
                    usage("-port requires an argument");
                }
            } else if (thisArg.equals("-useJAAS")) {
                useJAAS = true;
            } else {
                usage("Unknown argument: " + thisArg);
            }
        }

        /* Create the store configuration */
        kvConfig = new KVStoreConfig(storeName, hostName + ":" + hostPort);
    }

    private void usage(String message) {
        System.out.println("\n" + message + "\n");
        System.out.println("usage: " + getClass().getName());
        System.out.println("\t-store <instance name> " +
                           "-host <host name> " +
                           "-port <port number> " +
                           "[-useJAAS]");
        System.exit(1);
    }

    private Properties generateSecProperties() {

        /* Create the required security properties */
        final Properties securityProps = new Properties();

        /* Set the user name */
        securityProps.setProperty(KVSecurityConstants.AUTH_USERNAME_PROPERTY,
                                  "krbuser");

        /* Use Kerberos */
        securityProps.setProperty(KVSecurityConstants.AUTH_EXT_MECH_PROPERTY,
                                  "kerberos");

        /* Set SSL for the wire level encryption */
        securityProps.setProperty(KVSecurityConstants.TRANSPORT_PROPERTY,
                                  KVSecurityConstants.SSL_TRANSPORT_NAME);

        /* Set the location of the public trust file for SSL */
        final String trustStore = "/tmp/store.trust";
        securityProps.setProperty(
            KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY, trustStore);

        /* Set the service principal associated with the helper host */
        final String servicesDesc =
            "localhost:oraclenosql/localhost@EXAMPLE.COM";
        securityProps.setProperty(
            KVSecurityConstants.AUTH_KRB_SERVICES_PROPERTY, servicesDesc);

        /*
         * Set the default realm name to permit using a short name for the user
         * principal
         */
        securityProps.setProperty(KVSecurityConstants.AUTH_KRB_REALM_PROPERTY,
                                  "EXAMPLE.COM");

        return securityProps;
    }

    private void runExample()
        throws LoginException, PrivilegedActionException {

        final KVStore store;
        if (useJAAS) {
            store = runJAASExample();
        } else {
            store = runSecLoginExample();
        }

        System.out.println("Login to Kerberos based secured kvstore: ");

        final String keyString = "Hello";
        final String valueString = "Big Data World!";

        store.put(Key.createKey(keyString),
                  Value.createValue(valueString.getBytes()));

        final ValueVersion valueVersion = store.get(Key.createKey(keyString));

        System.out.println(keyString + " " +
                           valueVersion.getValue().getValue());

        store.close();
    }

    private KVStore runJAASExample()
        throws LoginException, PrivilegedActionException {

        final Subject subj = new Subject();

        /* Use the oraclenosql entry in the JAAS configuration */
        final LoginContext lc = new LoginContext("oraclenosql", subj);

        /* Attempt authentication */
        lc.login();

        /* Set the required security properties in the configuration */
        kvConfig.setSecurityProperties(generateSecProperties());

        return Subject.doAs(subj, new PrivilegedExceptionAction<KVStore>() {
            @Override
            public KVStore run() throws Exception {
                return KVStoreFactory.getStore(kvConfig);
            }
        });
    }

    private KVStore runSecLoginExample() {
        final Properties props = generateSecProperties();

        /* Specify the client keytab file location */
        props.setProperty(KVSecurityConstants.AUTH_KRB_KEYTAB_PROPERTY,
                          "/tmp/krbuser.keytab");

        /* Set the security properties in the configuration */
        kvConfig.setSecurityProperties(props);

        return KVStoreFactory.getStore(kvConfig);
    }
}
