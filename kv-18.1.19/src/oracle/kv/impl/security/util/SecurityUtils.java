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
package oracle.kv.impl.security.util;

import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_CLIENT;
import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_INTERNAL;
import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_JE_HA;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertPathBuilder;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.x500.X500Principal;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.SecurityConfigCreator.IOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig.ParamSetting;

/**
 * A collection of security-related utilities.
 */
public final class SecurityUtils {

    public static final String KEY_CERT_FILE = "certFileName";
    private static final String CERT_FILE_DEFAULT = "store.cert";

    public static final String KEY_KEY_ALGORITHM = "keyAlgorithm";
    private static final String KEY_ALGORITHM_DEFAULT = "RSA";

    public static final String KEY_KEY_SIZE = "keySize";
    private static final String KEY_SIZE_DEFAULT = "1024";

    public static final String KEY_DISTINGUISHED_NAME = "distinguishedName";
    private static final String DISTINGUISHED_NAME_DEFAULT = "cn=NoSQL";

    public static final String KEY_KEY_ALIAS = "keyAlias";
    public static final String KEY_ALIAS_DEFAULT = "shared";

    public static final String KEY_VALIDITY = "validity";
    private static final String VALIDITY_DEFAULT = "365";

    /*
     * The list of preferred protocols.  Both KV and JE SSL implementations
     * will filter out any that are not supported.  If none are supported,
     * an exception will be thrown.
     */
    public static final String PREFERRED_PROTOCOLS_DEFAULT =
        "TLSv1.2,TLSv1.1,TLSv1";

    /*
     * The strings used by the keystore utility.  Probably subject to
     * localization, etc..
     */
    private static final String KS_PRIVATE_KEY_ENTRY = "PrivateKeyEntry";
    private static final String KS_SECRET_KEY_ENTRY = "SecretKeyEntry";
    private static final String KS_TRUSTED_CERT_ENTRY = "trustedCertEntry";

    private static final String TEMP_CERT_FILE = "temp.cert";

    /*
     * The strings used by the Kerberos utility.
     */
    public static final String KADMIN_DEFAULT = "/usr/kerberos/sbin/kadmin";
    public static final String KRB_CONF_FILE = "/etc/krb5.conf";

    private static final String PRINCIPAL_VALIDITY = "krbPrincValidity";
    private static final String PRINC_VALIDITY_DEFAULT = "365days";

    private static final String KEYSALT_LIST = "krbKeysalt";
    private static final String PRINCIPAL_PWD_EXPIRE = "krbPrincPwdExpire";

    private static final String PRINC_PWD_EXPIRE_DEFAULT = "365days";
    private static final String KEYSALT_LIST_DEFAULT = "des3-cbc-sha1:normal," +
        "aes128-cts-hmac-sha1-96:normal,arcfour-hmac:normal";
    public static final String KERBEROS_AUTH_NAME = "KERBEROS";
    public static final String KRB_NAME_COMPONENT_SEPARATOR_STR = "/";
    public static final String KRB_NAME_REALM_SEPARATOR_STR = "@";

    public static final Properties princDefaultProps = new Properties();

    /* The strings used by the IDCS OAuth */
    public static final String OAUTH_AUTH_NAME = "IDCSOAUTH";

    private static final String digitSet = "0123456789";
    private static final String upperSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String lowerSet = "abcdefghijklmnopqrstuvwxyz";
    private static final String specialSet = "!#$%&'()*+,-./:; <>?@[]^_`{|}~";
    private static final String allCharSet = digitSet + upperSet +
        lowerSet + specialSet;

    private static final SecureRandom random = new SecureRandom();

    /* User id prefix used for creating user principal */
    public static final String IDCS_OAUTH_USER_ID_PREFIX = "idcs";

    static {
        princDefaultProps.put(PRINCIPAL_VALIDITY, PRINC_VALIDITY_DEFAULT);
        princDefaultProps.put(KEYSALT_LIST, KEYSALT_LIST_DEFAULT);
        princDefaultProps.put(PRINCIPAL_PWD_EXPIRE, PRINC_PWD_EXPIRE_DEFAULT);
    }

    private static final Set<String> preferredProtocols = new HashSet<>();

    static {
        preferredProtocols.add("TLSv1.2");
        preferredProtocols.add("TLSv1.1");
        preferredProtocols.add("TLSv1");
    }

    /* not instantiable */
    private SecurityUtils() {
    }

    /**
     * Given an abstract file, attempt to change permissions so that it is
     * readable only by the owner of the file.
     * @param f a File referencing a file or directory on which permissions are
     * to be changed.
     * @return true if the permissions were successfully changed
     */
    public static boolean makeOwnerAccessOnly(File f)
        throws IOException {

        if (!f.exists()) {
            return false;
        }

        final FileSysUtils.Operations osOps = FileSysUtils.selectOsOperations();
        return osOps.makeOwnerAccessOnly(f);
    }

    /**
     * Given an abstract file, attempt to change permissions so that it is
     * writable only by the owner of the file.
     * @param f a File referencing a file or directory on which permissions are
     * to be changed.
     * @return true if the permissions were successfully changed
     */
    public static boolean makeOwnerOnlyWriteAccess(File f)
        throws IOException {

        if (!f.exists()) {
            return false;
        }

        final FileSysUtils.Operations osOps = FileSysUtils.selectOsOperations();
        return osOps.makeOwnerAccessOnly(f);
    }

    public static boolean passwordsMatch(char[] pwd1, char[] pwd2) {
        if (pwd1 == pwd2) {
            return true;
        }

        if (pwd1 == null || pwd2 == null) {
            return false;
        }

        return Arrays.equals(pwd1, pwd2);
    }

    public static void clearPassword(char[] pwd) {
        if (pwd != null) {
            for (int i = 0; i < pwd.length; i++) {
                pwd[i] = ' ';
            }
        }
    }

    /**
     * Make a java keystore and an associated trustStore.
     * @param securityDir the directory in which the keystore and truststore
     *    will be created.
     * @param sp a SecurityParams instance containing information regarding
     * the keystore and truststore file names
     * @param keyStorePassword the password with which the keystore and
     * truststore will be secured
     * @param props a set of optional settings that can alter the
     *    keystore creation.
     * @return true if the creation process was successful and false
     *    if an error occurred.
     */
    public static boolean initKeyStore(File securityDir,
                                       SecurityParams sp,
                                       char[] keyStorePassword,
                                       Properties props) {
        if (props == null) {
            props = new Properties();
        }

        final String certFileName = props.getProperty(KEY_CERT_FILE,
                                                      CERT_FILE_DEFAULT);

        final String keyStoreFile =
            new File(securityDir.getPath(), sp.getKeystoreFile()).getPath();
        final String keyStoreType = sp.getKeystoreType();
        final String trustStoreFile =
            new File(securityDir.getPath(), sp.getTruststoreFile()).getPath();
        final String trustStoreType = sp.getTruststoreType();
        final String certFile =
            new File(securityDir.getPath(), certFileName).getPath();

        try {
            final String keyAlg = props.getProperty(KEY_KEY_ALGORITHM,
                                                    KEY_ALGORITHM_DEFAULT);
            final String keySize = props.getProperty(KEY_KEY_SIZE,
                                                     KEY_SIZE_DEFAULT);
            final String dname = props.getProperty(KEY_DISTINGUISHED_NAME,
                                                   DISTINGUISHED_NAME_DEFAULT);
            final String keyAlias = props.getProperty(KEY_KEY_ALIAS,
                                                      KEY_ALIAS_DEFAULT);
            /*
             * TODO: converting to String here introduces some security risk.
             * Consider changing the keytool invocation to respond directly to
             * the password prompt rather an converting to String and sticking
             * on the command line.  In the meantime, this is a relatively low
             * security risk since it is only used in one-shot setup commands.
             */
            final String keyStorePasswordStr = new String(keyStorePassword);
            final String validityDays = props.getProperty(KEY_VALIDITY,
                                                          VALIDITY_DEFAULT);

            final String[] keyStoreCmds = new String[] {
                "keytool",
                "-genkeypair",
                "-keystore", keyStoreFile,
                "-storetype", keyStoreType,
                "-storepass", keyStorePasswordStr,
                "-keypass", keyStorePasswordStr,
                "-alias", keyAlias,
                "-dname", dname,
                "-keyAlg", keyAlg,
                "-keysize", keySize,
                "-validity", validityDays };
            int result = runCmd(keyStoreCmds);
            if (result != 0) {
                System.err.println(
                    "Error creating keyStore: return code " + result);
                return false;
            }

            final String[] exportCertCmds = new String[] {
                "keytool",
                "-export",
                "-file", certFile,
                "-keystore", keyStoreFile,
                "-storetype", keyStoreType,
                "-storepass", keyStorePasswordStr,
                "-alias", keyAlias };
            result = runCmd(exportCertCmds);

            if (result != 0) {
                System.err.println(
                    "Error exporting certificate: return code " + result);
                return false;
            }

            try {
                /*
                 * We will re-use the keystore password for the truststore
                 */
                final String[] importCertCmds = new String[] {
                    "keytool",
                    "-import",
                    "-file", certFile,
                    "-keystore", trustStoreFile,
                    "-storetype", trustStoreType,
                    "-storepass", keyStorePasswordStr,
                    "-noprompt" };
                result = runCmd(importCertCmds);

                if (result != 0) {
                    System.err.println(
                        "Error importing certificate to trustStore: " +
                        "return code " + result);
                    return false;
                }
            } finally {
                /* Delete the cert file when done - we no longer need it */
                new File(certFile).delete();
            }

            makeOwnerOnlyWriteAccess(new File(keyStoreFile));
            makeOwnerOnlyWriteAccess(new File(trustStoreFile));

        } catch (IOException ioe) {
            System.err.println("IO error encountered: " + ioe.getMessage());
            return false;
        }


        return true;
    }

    /**
     * Update the security parameters in the given security directory.
     * @param secDir security configuration directory
     * @param params list of security parameters to update
     * @throws IllegalStateException if an error occurs in the update process
     */
    public static void updateSecurityParams(File secDir,
                                            List<ParamSetting> params) {
        final File secXmlFile =
            new File(secDir, FileNames.SECURITY_CONFIG_FILE);
        if (!secXmlFile.exists()) {
            throw new IllegalStateException(
                "security.xml file does not exist, " +
                "cannot update the security parameters");
        }

        /* Get original security parameters */
        final SecurityParams sp = loadSecurityParams(secDir);
        applyParamsChanges(sp, params);
        ConfigUtils.createSecurityConfig(sp,secXmlFile);
    }

    public static void applyParamsChanges(SecurityParams sp,
                                          List<ParamSetting> paramSettings) {
        final ParameterMap pmap = sp.getMap();
        for (ParamSetting setting : paramSettings) {
            final ParameterState pstate = setting.getParameterState();
            if (pstate.appliesTo(Info.TRANSPORT)) {
                if (setting.getTransportName() == null) {
                    for (ParameterMap tmap : sp.getTransportMaps()) {
                        tmap.setParameter(setting.getParamName(),
                                          setting.getParamValue());
                    }
                } else {
                    final ParameterMap tmap =
                        sp.getTransportMap(setting.getTransportName());
                    tmap.setParameter(setting.getParamName(),
                                      setting.getParamValue());
                }
            } else {
                pmap.setParameter(setting.getParamName(),
                                  setting.getParamValue());
            }
        }
    }

    /**
     * Merges the trust information from srcSecDir into updateSecDir.
     * @param srcSecDir a File reference to the security directory from which
     *   trust information will be extracted
     * @param updateSecDir a File reference to the security directory into
     *   which trust information will be merged
     * @return true if the merge was successful and false otherwise
     */
    public static boolean mergeTrust(File srcSecDir,
                                     File updateSecDir) {

        /* Get source truststore info */
        final SecurityParams srcSp = loadSecurityParams(srcSecDir);
        final String srcTrustFile =
            new File(srcSecDir, srcSp.getTruststoreFile()).getPath();
        /*
         * TODO: converting to String here introduces some security risk.
         * Consider changing the keytool invocation to respond directly to
         * the password prompt rather an converting to String and sticking
         * on the command line.  In the meantime, this is a relatively low
         * security risk since it is only used in one-shot setup commands.
         */

        final String srcTruststorePwd =
            new String(retrieveKeystorePassword(srcSp));
        final String srcTruststoreType = srcSp.getTruststoreType();
        final List<KeystoreEntry> stEntries =
            listKeystore(new File(srcTrustFile), srcTruststoreType,
                         srcTruststorePwd);

        /* Get dest truststore info */
        final SecurityParams updateSp = loadSecurityParams(updateSecDir);
        final String updateTrustFile =
            new File(updateSecDir, updateSp.getTruststoreFile()).getPath();
        final String updateTruststorePwd =
            new String(retrieveKeystorePassword(updateSp));
        final String updateTruststoreType = updateSp.getTruststoreType();
        final List<KeystoreEntry> utEntries =
            listKeystore(new File(updateTrustFile), updateTruststoreType,
                         updateTruststorePwd);

        /*
         * Convert the to-be-updated list to a set of alias names for
         * ease of later access.
         */
        final Set<String> utAliasSet = new HashSet<String>();
        for (KeystoreEntry entry : utEntries) {
            utAliasSet.add(entry.getAlias());
        }

        /* The file to hold the temporary cert */
        final String certFileName = TEMP_CERT_FILE;
        final String certFile =
            new File(srcSecDir.getPath(), certFileName).getPath();

        try {

            for (KeystoreEntry entry : stEntries) {
                final String[] exportCertCmds = new String[] {
                    "keytool",
                    "-export",
                    "-file", certFile,
                    "-keystore", srcTrustFile,
                    "-storetype", srcTruststoreType,
                    "-storepass", srcTruststorePwd,
                    "-alias", entry.getAlias() };
                int result = runCmd(exportCertCmds);

                if (result != 0) {
                    System.err.println(
                        "Error exporting certificate: return code " + result);
                    return false;
                }

                /*
                 * Determine an available alias
                 */
                String alias = entry.getAlias();
                if (utAliasSet.contains(alias)) {
                    int i = 2;
                    while (true) {
                        final String tryAlias = alias + "_" + i;
                        if (!utAliasSet.contains(tryAlias)) {
                            alias = tryAlias;
                            break;
                        }
                        i++;
                    }
                }
                utAliasSet.add(alias);

                final String[] importCertCmds = new String[] {
                    "keytool",
                    "-import",
                    "-file", certFile,
                    "-alias", alias,
                    "-keystore", updateTrustFile,
                    "-storetype", updateTruststoreType,
                    "-storepass", updateTruststorePwd,
                    "-noprompt" };
                result = runCmd(importCertCmds);

                if (result != 0) {
                    System.err.println(
                        "Error importing certificate to trustStore: " +
                        "return code " + result);
                    return false;
                }
            }
        } catch (IOException ioe) {
            System.err.println(
                "Exception " + ioe + " while merging truststore files");
            return false;
        }

        /*
         * Copy the new truststore file to a client.trust file so that the
         * two are consistent.
         */
        final File srcFile = new File(updateTrustFile);
        final File dstFile =
            new File(updateSecDir, FileNames.CLIENT_TRUSTSTORE_FILE);
        try {
            SecurityUtils.copyOwnerWriteFile(srcFile, dstFile);
        } catch (IOException ioe) {
            System.err.println(
                "Exception " + ioe + " while copying " + srcFile +
                " to " + dstFile);
            return false;
        }

        return true;
    }

    public static class KeystoreEntry {
        private final String alias;
        private final EntryType entryType;

        public enum EntryType {
            PRIVATE_KEY,
            SECRET_KEY,
            TRUSTED_CERT,
            OTHER;
        }

        public KeystoreEntry(String alias, EntryType entryType) {
            this.alias = alias;
            this.entryType = entryType;
        }

        String getAlias() {
            return alias;
        }

        EntryType getEntryType() {
            return entryType;
        }
    }

    /**
     * Print out entries in keystores of given security configuration
     * directory.
     * @param secConfigDir security configuration directory.
     * @return a string of entries information in keystores if successful,
     *         or null otherwise.
     */
    public static String printKeystores(File secConfigDir) {
        final SecurityParams sp = loadSecurityParams(secConfigDir);
        final String keystoreFile =
            new File(secConfigDir, sp.getKeystoreFile()).getPath();
        final String truststoreFile =
            new File(secConfigDir, sp.getTruststoreFile()).getPath();
        final String keystorePwd = new String(retrieveKeystorePassword(sp));
        final String keystoreType = sp.getKeystoreType();
        final String truststoreType = sp.getTruststoreType();

        return printKeystore(keystoreFile, keystoreType, keystorePwd) + "\n" +
            printKeystore(truststoreFile, truststoreType, keystorePwd);
    }

    private static String printKeystore(String keystoreFile,
                                        String keystoreType,
                                        String keystorePwd) {
        final StringBuilder sb = new StringBuilder();
        try {
            final String[] keyStoreCmds = new String[] {
                    "keytool",
                    "-list",
                    "-keystore", keystoreFile,
                    "-storetype", keystoreType,
                    "-storepass", keystorePwd };

            final List<String> output = new ArrayList<String>();
            final int result = runCmd(keyStoreCmds, output);
            if (result != 0) {
                sb.append("Error listing keyStore: ").append(output);
                return sb.toString();
            }
            sb.append("Keystore: " + keystoreFile + "\n");

            for (String s : output) {
                sb.append(s + "\n");
            }
            return sb.toString();
        } catch (IOException ioe) {
            sb.append("IO error encountered: ").append(ioe.getMessage());
            return sb.toString();
        }
    }

    /**
     * List the entries in a keystore (or truststore) file.
     * @param keystoreFile the keystore file
     * @param keystoreType the type of the keystore file
     * @param storePassword the password for the store
     * @return a list of the keystore entries if successful, or null otherwise
     */
    public static List<KeystoreEntry> listKeystore(File keystoreFile,
                                                   String keystoreType,
                                                   String storePassword) {
        try {
            final String[] keyStoreCmds = new String[] {
                "keytool",
                "-list",
                "-keystore", keystoreFile.getPath(),
                "-storetype", keystoreType,
                "-storepass", storePassword };

            final List<String> output = new ArrayList<String>();
            final int result = runCmd(keyStoreCmds, output);
            if (result != 0) {
                System.err.println(
                    "Error listing keyStore: return code " + result);
                return null;
            }

            final Pattern keystoreContains =
                Pattern.compile("Your keystore contains ([0-9]+) entr.*");

            /*
             * Entries look like this:
             * shared, Dec 31, 2013, PrivateKeyEntry,
             */
            final Pattern entryPattern =
                Pattern.compile("([^,]+),([^,]+, [0-9]+, )([a-zA-Z]+),.*");

            final List<KeystoreEntry> entries = new ArrayList<KeystoreEntry>();
            boolean startFound = false;
            for (String s : output) {
                if (!startFound) {
                    final Matcher m = keystoreContains.matcher(s);
                    if (m.matches()) {
                        startFound = true;
                    }
                } else {
                    final Matcher m = entryPattern.matcher(s);
                    if (m.matches()) {
                        final String entryTypeStr = m.group(3);
                        final KeystoreEntry.EntryType entryType;
                        if (entryTypeStr.equals(KS_PRIVATE_KEY_ENTRY)) {
                            entryType = KeystoreEntry.EntryType.PRIVATE_KEY;
                        } else if (entryTypeStr.equals(KS_SECRET_KEY_ENTRY)) {
                            entryType = KeystoreEntry.EntryType.SECRET_KEY;
                        } else if (entryTypeStr.equals(KS_TRUSTED_CERT_ENTRY)) {
                            entryType = KeystoreEntry.EntryType.TRUSTED_CERT;
                        }  else {
                            entryType = KeystoreEntry.EntryType.OTHER;
                        }
                        entries.add(new KeystoreEntry(m.group(1), entryType));
                    }
                }
            }
            return entries;
        } catch (IOException ioe) {
            System.err.println("IO error encountered: " + ioe.getMessage());
            return null;
        }
    }

    /**
     * Make a copy of a file where the resulting copy should be writable only
     * by the owner with read privilege determined by system policy
     * (i.e. umask).
     * @param srcFile a file to be copied
     * @param destFile the destination file
     * @throws IOException if an error occurs in the copy process
     */
    public static void copyOwnerWriteFile(File srcFile, File destFile)
        throws IOException {

        FileUtils.copyFile(srcFile, destFile);
        SecurityUtils.makeOwnerOnlyWriteAccess(destFile);
    }

    /**
     * Create store service principal and extract keytab file.
     *
     * @param securityDir the directory in which the keytab will be created.
     * @param sp a SecurityParams instance containing information regarding
     *        the store service principal and keytab file names
     * @param kadminSetting kadmin settings
     * @param props a set of optional settings that can alter the
     *        principal creation, or null
     * @param ioHelper I/O helper class used to read kadmin password
     * @return true if the generation process was successful and false
     *         if an error occurred.
     */
    public static boolean generateKeyTabFile(File securityDir,
                                             SecurityParams sp,
                                             KadminSetting kadminSetting,
                                             Properties props,
                                             IOHelper ioHelper) {
        if (props == null) {
            props = new Properties();
        }

        final String keytabFile = new File(
            securityDir.getPath(), sp.getKerberosKeytabFile()).getPath();

        try {
            final String princName = sp.getKerberosServiceName();
            final String validityDays = props.getProperty(PRINCIPAL_VALIDITY);
            final String keysaltList = props.getProperty(KEYSALT_LIST);
            final String pwdExpire = props.getProperty(PRINCIPAL_PWD_EXPIRE);
            final String instance = sp.getKerberosInstanceName();
            final String realm = sp.getKerberosRealmName();
            final String principal = (instance != null) ?
                                     princName + "/" + instance :
                                     princName;

            final List<String> kadminCmdsList =
                generateKadminCmds(kadminSetting, realm);

            /* Add store service principal */
            final String addPrincCmds = "add_principal" +
                " -expire " + validityDays +
                " -pwexpire " + pwdExpire +
                " -randkey " + "\"" + principal + "\"";
            kadminCmdsList.add(addPrincCmds);

            System.out.println("Adding principal " + principal);
            int result = runKadminCmd(kadminSetting, ioHelper, kadminCmdsList);
            if (result != 0) {
                System.err.println(
                    "Error adding service principal: return code " + result);
                return false;
            }
            kadminCmdsList.remove(kadminCmdsList.size() - 1);

            /* Extract keytab of service principal */
            System.out.println("Extracting keytab " + keytabFile);
            final String extractKeytabCmds = "ktadd" +
                " -k " + keytabFile +
                " -e " + keysaltList +
                " " + "\"" + principal + "\"";
            kadminCmdsList.add(extractKeytabCmds);
            result = runKadminCmd(kadminSetting, ioHelper, kadminCmdsList);
            if (result != 0) {
                System.err.println(
                    "Error extracting keytab file: return code " + result);
                return false;
            }

            makeOwnerOnlyWriteAccess(new File(keytabFile));
        } catch (IOException ioe) {
            System.err.println("IO error encountered: " + ioe.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Renew keytab file in given security directory.
     *
     * @param secDir security directory
     * @param keysaltList keysalt list used to generate new keytab file
     * @param kadminSetting settings for connecting kadmin
     * @param ioHelper I/O helper class to read kadmin password
     * @return true if the renew process was successful and false
     *         if an error occurred.
     */
    public static boolean renewKeytab(File secDir,
                                      String keysaltList,
                                      KadminSetting kadminSetting,
                                      IOHelper ioHelper) {
        final SecurityParams sp = loadSecurityParams(secDir);
        final File keytabFile = new File(secDir, sp.getKerberosKeytabFile());
        if (!keytabFile.exists()) {
            System.err.println("keytab " + keytabFile + " does not exist");
            return false;
        }
        final String principal = SecurityUtils.getCanonicalPrincName(sp);
        File tmpKeytab = null;
        try {
            final List<String> kadminCmdsList =
                generateKadminCmds(kadminSetting, sp.getKerberosRealmName());
            if (keysaltList == null) {
                keysaltList = KEYSALT_LIST_DEFAULT;
            }

            /*
             * Create a temporary file to store the new keys of principal,
             * so that old key can be reserved in case of extracting keytab
             * error.
             *
             * Using createTempFile method to create temporary file then
             * remove it and only keep the file name, since ktadd command
             * cannot store keys to a file generated by Java.
             */
            tmpKeytab = File.createTempFile("tmp", ".keytab");
            if (!tmpKeytab.delete()) {
                System.err.println("Error generating a temporary keytab file");
                return false;
            }
            final String extractKeytabCmds = "ktadd" +
                " -k " + tmpKeytab.getAbsolutePath() +
                " -e " + keysaltList +
                " " + principal;
            kadminCmdsList.add(extractKeytabCmds);
            final int result =
                runKadminCmd(kadminSetting, ioHelper, kadminCmdsList);
            if (result != 0) {
                System.err.println(
                    "Error extracting keytab file: return code " + result);
                return false;
            }
            if (!keytabFile.delete()) {
                System.err.println("Old keytab " + keytabFile +
                                   " cannot be deleted");
                return false;
            }
            if (!tmpKeytab.renameTo(keytabFile)) {
                System.err.println("keytab " + tmpKeytab +
                                   " cannot be renamed as " + keytabFile);
                return false;
            }
            makeOwnerOnlyWriteAccess(keytabFile);
        } catch (IOException ioe) {
            System.err.println("IO error encountered: " + ioe.getMessage());
            return false;
        } finally {
            if (tmpKeytab != null &&
                tmpKeytab.exists() &&
                !tmpKeytab.delete()) {
                System.err.println("Temporary keytab " + tmpKeytab +
                                   " cannot be deleted");
                return false;
            }
        }
        return true;
    }

    /**
     * Check given authentication method name is Kerberos.
     */
    public static boolean isKerberos(String authMethod) {
        if (authMethod == null) {
            return false;
        }
        return authMethod.equalsIgnoreCase(KERBEROS_AUTH_NAME);
    }

    /**
     * Check given authentication methods in format "authMethod1,authMethod2"
     * contains Kerberos.
     */
    public static boolean hasKerberos(String authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods.split(",")) {
            if (isKerberos(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check given authentication method name is IDCS OAuth.
     */
    private static boolean isIDCSOAuth(String authMethod) {
        if (authMethod == null) {
            return false;
        }
        return authMethod.equalsIgnoreCase(OAUTH_AUTH_NAME);
    }

    /**
     * Check given authentication methods in format "authMethod1,authMethod2"
     * contains IDCS OAuth.
     */
    public static boolean hasIDCSOAuth(String authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods.split(",")) {
            if (isIDCSOAuth(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check given authentication methods array contains element of IDCS OAuth.
     */
    public static boolean hasIDCSOAuth(String[] authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods) {
            if (isIDCSOAuth(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check given authentication methods array contains element of Kerberos.
     */
    public static boolean hasKerberos(String[] authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods) {
            if (isKerberos(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return canonical Kerberos service principal.
     */
    public static String getCanonicalPrincName(SecurityParams secParams) {
        final StringBuilder sb = new StringBuilder();
        sb.append(secParams.getKerberosServiceName());

        String instanceName = secParams.getKerberosInstanceName();
        if (instanceName != null && !instanceName.equals("")) {
            sb.append(KRB_NAME_COMPONENT_SEPARATOR_STR);
            sb.append(secParams.getKerberosInstanceName());
        }

        if (!secParams.getKerberosRealmName().equals("")) {
            sb.append(KRB_NAME_REALM_SEPARATOR_STR);
            sb.append(secParams.getKerberosRealmName());
        }
        return sb.toString();
    }

    private static List<String> generateKadminCmds(KadminSetting kadminSetting,
                                                   String defaultRealm) {
        final List<String> kadminCmdsList = new ArrayList<String>();
        kadminCmdsList.add(kadminSetting.getKrbAdminPath());
        kadminCmdsList.add("-r");
        kadminCmdsList.add(defaultRealm);

        if (kadminSetting.useKeytab()) {
            kadminCmdsList.add("-k");
            kadminCmdsList.add("-t");
            kadminCmdsList.add(kadminSetting.getKrbAdminKeytab());
            System.out.println(String.format(
                "Login Kerberos admin via keytab %s with %s",
                kadminSetting.getKrbAdminKeytab(),
                kadminSetting.getKrbAdminPrinc()));
        } else if (kadminSetting.useCcache()) {
            kadminCmdsList.add("-c");
            kadminCmdsList.add(kadminSetting.getKrbAdminCcache());
            System.out.println(String.format(
                "Login Kerberos admin via credential cache %s with %s",
                kadminSetting.getKrbAdminCcache(),
                kadminSetting.getKrbAdminPrinc()));
        }

        if (kadminSetting.getKrbAdminPrinc() != null) {
            kadminCmdsList.add("-p");
            kadminCmdsList.add(kadminSetting.getKrbAdminPrinc());
        }
        kadminCmdsList.add("-q");
        return kadminCmdsList;
    }

    /**
     * The class maintain Kerberos V5 configuration information that retrieved
     * from user specified configuration files. Copy the approach used by the
     * Java internal Kerberos parsing code to retrieve and validate default
     * realm and kdc, which is required for NoSQL Kerberos authentication.
     * The rest of configuration parameters are validated by Java Kerberos
     * login module when performing the actual authentication.
     */
    public static class Krb5Config {
        private File configFile;
        private String defaultRealm;
        private String realmKdc;

        public Krb5Config(File krb5ConfFile) {
            configFile = krb5ConfFile;
        }

        /**
         * Parse krb5 configuration file and identify default realm and
         * corresponding kdc.
         */
        public void parseConfigFile() throws IOException {
            final List<String> lines = loadConfigFile();
            final Map<String, String> kdcs = new HashMap<>();

            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i).trim();

                /* Find default realm from libdefaults */
                if (line.equalsIgnoreCase("[libdefaults]")) {
                    for (int count = i + 1; count < lines.size(); count++) {
                        line = lines.get(count).trim();

                        final int equalsPos = line.indexOf('=');
                        if (equalsPos > 0) {
                            final String key =
                                line.substring(0, equalsPos).trim();

                            if (key.equalsIgnoreCase("default_realm")) {
                                defaultRealm =
                                    trimmed(line.substring(equalsPos + 1));
                            }
                        }
                        if (lines.get(count).startsWith("[")) {
                            i = count - 1;
                            break;
                        }
                    }
                } else if (line.equalsIgnoreCase("[realms]")) {
                    /* Parse all realms and cache their corresponding kdc */
                    String realm = "";

                    for (int count = i + 1; count < lines.size(); count++) {
                        line = lines.get(count).trim();
                        if (line.endsWith("{")) {
                            final int equalsPos = line.indexOf('=');
                            if (equalsPos > 0) {
                                realm = line.substring(0, equalsPos).trim();
                            }
                        } else if (!line.startsWith("}")) {
                            final int equalsPos = line.indexOf('=');
                            if (equalsPos > 0) {
                                final String key =
                                    line.substring(0, equalsPos).trim();

                                if (key.equalsIgnoreCase("kdc") &&
                                    !realm.equals("")) {
                                    /*
                                     * User can specify multiple realms in
                                     * the configuration file, cache them
                                     * firstly and find the default kdc
                                     * later.
                                     */
                                    kdcs.put(realm, trimmed(
                                        line.substring(equalsPos + 1)));
                                }
                            }
                        }

                        if (lines.get(count).startsWith("[")) {
                            i = count - 1;
                            break;
                        }
                    }
                }
            }

            if (defaultRealm != null) {
                realmKdc = kdcs.get(defaultRealm);
            }
        }

        public String getDefaultRealm() {
            return defaultRealm;
        }

        public String getKdc() {
            return realmKdc;
        }

        public String getConfigFilePath() {
            return configFile.getAbsolutePath();
        }

        private String trimmed(String s) {
            s = s.trim();
            if (s.charAt(0) == '"' && s.charAt(s.length()-1) == '"' ||
                s.charAt(0) == '\'' && s.charAt(s.length()-1) == '\'') {
                s = s.substring(1, s.length()-1).trim();
            }
            return s;
        }

        private List<String> loadConfigFile() throws IOException {
            final List<String> lines = new ArrayList<String>();

            try (final BufferedReader br = new BufferedReader(
                     new InputStreamReader(new FileInputStream(configFile)))) {
                String line;

                /*
                 * Cache previous line, used to resolve the case that Kerberos
                 * configuration file accepts and convert to standard format.
                 *  EXAMPLE.COM =
                 *  {
                 *      kdc = kerberos.example.com
                 *  }
                 */
                String previous = null;

                while ((line = br.readLine()) != null) {
                    /* Ignore comments and blank lines */
                    if (!(line.startsWith("#") || line.trim().isEmpty())) {
                        String current = line.trim();

                        if (current.startsWith("{")) {
                            if (previous == null) {
                                throw new IOException(
                                     "Config file should not start with \"{\"");
                            }
                            previous += " " + current;
                        } else {
                            if (previous != null) {
                                lines.add(previous);
                            }
                            previous = current;
                        }
                    }
                }

                if (previous != null) {
                    lines.add(previous);
                }
                return lines;
            }
        }
    }

    /**
     * The class maintains the setting used to connecting kadmin utility.
     */
    public static class KadminSetting {
        private static final String NO_KADMIN = "NONE";
        private String krbAdminPath = KADMIN_DEFAULT;
        private String krbAdminPrinc;
        private String krbAdminKeytab;
        private String krbAdminCcache;

        public KadminSetting setKrbAdminPath(String kadminPath) {
            this.krbAdminPath = kadminPath;
            return this;
        }

        public String getKrbAdminPath() {
            return krbAdminPath;
        }

        public KadminSetting setKrbAdminPrinc(String kadminPrinc) {
            this.krbAdminPrinc = kadminPrinc;
            return this;
        }

        public String getKrbAdminPrinc() {
            return krbAdminPrinc;
        }

        public KadminSetting setKrbAdminKeytab(String adminKeytab) {
            this.krbAdminKeytab = adminKeytab;
            return this;
        }

        public String getKrbAdminKeytab() {
            return krbAdminKeytab;
        }

        public KadminSetting setKrbAdminCcache(String adminCcache) {
            this.krbAdminCcache = adminCcache;
            return this;
        }

        public String getKrbAdminCcache() {
            return krbAdminCcache;
        }

        public boolean doNotPerformKadmin() {
            return krbAdminPath.equalsIgnoreCase(NO_KADMIN);
        }

        /**
         * Validate if given kadmin settings are appropriate.
         *
         * @throws IllegalArgumentException
         */
        public void validateKadminSetting()
            throws IllegalArgumentException {

            if (doNotPerformKadmin()) {
                return;
            }

            /*
             * Check if user specified admin keytab and credential cache at the
             * same time
             */
            if (krbAdminKeytab != null) {
                if (krbAdminCcache != null) {
                    throw new IllegalArgumentException(
                        "cannot use admin ketyab and credential cache together");
                }

                if (krbAdminPrinc == null) {
                    throw new IllegalArgumentException(
                        "must specify admin principal when using keytab file");
                }

                if (!new File(krbAdminKeytab).exists()) {
                    throw new IllegalArgumentException(
                        "keytab file " + krbAdminKeytab + " does not exist");
                }
            }

            /* check if kadmin ccache exists */
            if (krbAdminCcache != null && !new File(krbAdminCcache).exists()) {
                throw new IllegalArgumentException(
                    "credential cache " + krbAdminCcache + " does not exist");
            }

            /* Must specify principal if use password */
            if (krbAdminKeytab == null &&
                krbAdminCcache == null &&
                krbAdminPrinc == null) {
                throw new IllegalArgumentException("use kadmin with password " +
                    "must specify principal name");
            }
        }

        /**
         * Whether use keytab to connect kadmin utility.
         */
        public boolean useKeytab() {
            return krbAdminKeytab != null &&
                   krbAdminPrinc != null &&
                   krbAdminCcache == null;
        }

        /**
         * Whether use credential cache to connect kadmin utility.
         */
        public boolean useCcache() {
            return krbAdminCcache != null &&
                   krbAdminKeytab == null;
        }

        /**
         * Whether prompt password to connect kadmin utility.
         */
        public boolean promptPwd() {
            return krbAdminCcache == null && krbAdminKeytab == null;
        }
    }

    /**
     * Run a command in a subshell.
     * @param args an array of command-line arguments, in the format expected
     * by Runtime.exec(String[]).
     * @return the process exit code
     * @throws IOException if an IO error occurs during the exec process
     */
    static int runCmd(String[] args)
        throws IOException {
        final Process proc = Runtime.getRuntime().exec(args);

        boolean done = false;
        int returnCode = 0;
        while (!done) {
            try {
                returnCode = proc.waitFor();
                done = true;
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        return returnCode;
    }

    /**
     * Run kadmin-related commands. If users do not specify keytab or credential
     * cache for kadmin user, prompt for admin user password.
     *
     * @param setting settings for connecting kadmin utility
     * @param ioHelper I/O helper may be used for password prompt
     * @param args an list of command line arguments.
     * @return the process exit code
     * @throws IOException if an IO error occurs during the process execution.
     */
    private static int runKadminCmd(KadminSetting setting,
                                    IOHelper ioHelper,
                                    List<String> args)
        throws IOException {

        final List<String> output = new ArrayList<>();
        final ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        char[] pwd = null;

        final Process proc = pb.start();
        final BufferedReader br =
            new BufferedReader(new InputStreamReader(proc.getInputStream()));

        if (setting.promptPwd()) {
            final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(proc.getOutputStream()));

            pwd = ioHelper.readPassword(
                "Password for " + setting.getKrbAdminPrinc() + ": ");
            if (pwd == null) {
                System.err.println("Failed to acquire kadmin password");
            }
            writer.write(pwd);
            SecurityUtils.clearPassword(pwd);
            writer.write("\n");
            writer.flush();
        }

        /* Read lines of input */
        boolean done = false;
        while (!done) {
            final String s = br.readLine();
            if (s == null) {
                done = true;
            } else {
                output.add(s);
            }
        }

        /* Then get exit code */
        done = false;
        int returnCode = 0;
        while (!done) {
            try {
                returnCode = proc.waitFor();
                done = true;
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        /* Output kadmin error and std output for easier debugging */
        for (String s : output) {
            System.err.println(s);
        }
        return returnCode;
    }

    private static int runCmd(String[] args, List<String> output)
        throws IOException {

        final Process proc = Runtime.getRuntime().exec(args);
        final BufferedReader br =
            new BufferedReader(new InputStreamReader(proc.getInputStream()));

        /* Read lines of input */
        boolean done = false;
        while (!done) {
            final String s = br.readLine();
            if (s == null) {
                done = true;
            } else {
                output.add(s);
            }
        }

        /* Then get exit code */
        done = false;
        int returnCode = 0;
        while (!done) {
            try {
                returnCode = proc.waitFor();
                done = true;
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        return returnCode;
    }

    /**
     * Report whether the host parameter is an address that is local to this
     * machine. If the host is a name rather than a literal address, all
     * resolutions of the name must be local in order for the host to be
     * considered local.
     *
     * @param host either an IP address literal or host name
     * @return true it the host represents a local address
     * @throws SocketException if an IO exception occurs
     */
    public static boolean isLocalHost(String host)
        throws SocketException {

        try {
            boolean anyLocal = false;
            for (InetAddress hostAddr : InetAddress.getAllByName(host)) {
                if (isLocalAddress(hostAddr)) {
                    anyLocal = true;
                } else {
                    return false;
                }
            }
            return anyLocal;
        } catch (UnknownHostException uhe) {
            return false;
        }
    }

    /**
     * Determine whether the address portion of the InetAddress (host name is
     * ignored) is an address that is local to this machine.
     */
    private static boolean isLocalAddress(InetAddress address)
        throws SocketException {

        final Enumeration<NetworkInterface> netIfs =
            NetworkInterface.getNetworkInterfaces();
        while (netIfs.hasMoreElements()) {
            final NetworkInterface netIf = netIfs.nextElement();
            if (isLocalAddress(netIf, address)) {
                return true;
            }

            final Enumeration<NetworkInterface> subIfs =
                netIf.getSubInterfaces();
            while (subIfs.hasMoreElements()) {
                if (isLocalAddress(subIfs.nextElement(), address)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Determine whether the address portion of the InetAddress (host name is
     * ignored) is an address that is local to a network interface.
     */
    private static boolean isLocalAddress(NetworkInterface netIf,
                                          InetAddress address) {

        final Enumeration<InetAddress> addrs = netIf.getInetAddresses();
        while (addrs.hasMoreElements()) {
            final InetAddress addr = addrs.nextElement();
            if (addr.equals(address)) {
                return true;
            }
        }
        return false;
    }

    public static SecurityParams loadSecurityParams(File secDir) {
        final File secFile = new File(secDir, FileNames.SECURITY_CONFIG_FILE);
        return ConfigUtils.getSecurityParams(secFile);
    }

    private static char[] retrieveKeystorePassword(SecurityParams sp) {
        final KeyStorePasswordSource pwdSrc = KeyStorePasswordSource.create(sp);
        return (pwdSrc == null) ? null : pwdSrc.getPassword();
    }

    /**
     * Constructs a resource owner from KVStore user in current context.  Null
     * will be return if we could not detect a user principal in current
     * execution context, or the current execution context is null.
     */
    public static ResourceOwner currentUserAsOwner() {
        final KVStoreUserPrincipal currentUserPrinc =
            KVStoreUserPrincipal.getCurrentUser();
        if (currentUserPrinc == null) {
            return null;
        }
        return new ResourceOwner(currentUserPrinc.getUserId(),
                                 currentUserPrinc.getName());
    }

    /**
     * Return the default Kerberos principal configuration properties. These
     * properties are used whiling adding principal and extracting keytab.
     */
    public static Properties getDefaultKrbPrincipalProperties() {
        return (Properties) princDefaultProps.clone();
    }

    /**
     * Run various verifications against given security configuration directory.
     *
     * @param secConfigDir security configuration directory
     */
    public static String verifyConfiguration(File secConfigDir) {

        final SecurityParams sp = loadSecurityParams(secConfigDir);
        final StringBuilder errors = new StringBuilder();
        /* Verify security parameters */

        /*
         * Check that JE HA, internal, and client transports are using
         * preferred protocols.
         */

        /*
         * JE HA transport does not have notion of client and server: the
         * allowed protocols applies to both of them.
         */
        try {
            final String specifiedProtocols =
                sp.getTransAllowProtocols(SECURITY_TRANSPORT_JE_HA);
            if (!checkIfProtocolsAllowed(specifiedProtocols)) {
                errors.append(
                    "Transport JE HA is not using preferred protocols.")
                    .append(" Found: ")
                    .append(specifiedProtocols)
                    .append(" Preferred protocols: ")
                    .append(PREFERRED_PROTOCOLS_DEFAULT);
            }
        } catch (IllegalArgumentException iae) {
            errors.append(
                "Problem with protocols specified for transport JE HA: ")
                .append(iae.getMessage());
        }

        /* Internal and client transport only have client allowed protocols */
        String verifyResult = checkClientAllowedProtocols(
            SECURITY_TRANSPORT_INTERNAL, sp);
        if (verifyResult != null) {
            errors.append(verifyResult);
        }
        verifyResult = checkClientAllowedProtocols(
            SECURITY_TRANSPORT_CLIENT, sp);
        if (verifyResult != null) {
            errors.append(verifyResult);
        }

        /* Check if all transport are using the same key alias */
        final String serverInternalKeyAlias =
            sp.getTransServerKeyAlias(SECURITY_TRANSPORT_INTERNAL);
        final String clientInternalKeyAlias =
            sp.getTransClientKeyAlias(SECURITY_TRANSPORT_INTERNAL);
        if (!serverInternalKeyAlias.equals(clientInternalKeyAlias)) {
            errors.append("Key alias of internal transport server ")
                .append(serverInternalKeyAlias)
                .append(" is not the same as client ")
                .append(clientInternalKeyAlias)
                .append(".\n");
        }
        final String jeHaKeyAlias =
            sp.getTransServerKeyAlias(SECURITY_TRANSPORT_JE_HA);
        if (!serverInternalKeyAlias.equals(jeHaKeyAlias)) {
            errors.append("Key alias of internal transport server ")
                .append(serverInternalKeyAlias)
                .append(" is not the same as JE HA transport ")
                .append(jeHaKeyAlias)
                .append(".\n");
        }
        final String clientServerKeyAlias =
            sp.getTransServerKeyAlias(SECURITY_TRANSPORT_CLIENT);
        if (!serverInternalKeyAlias.equals(clientServerKeyAlias)) {
            errors.append("Key alias of internal transport server ")
                .append(serverInternalKeyAlias)
                .append(" is not the same as server for client transport ")
                .append(clientServerKeyAlias)
                .append(".\n");
        }

        /* Check if all transport are configured to allow the same identity */
        final String clientInternalAllowedIdentity =
            sp.getTransClientIdentityAllowed(SECURITY_TRANSPORT_INTERNAL);
        final String serverAllowedIdentity =
            sp.getTransServerIdentityAllowed(SECURITY_TRANSPORT_INTERNAL);
        if (!clientInternalAllowedIdentity.equals(serverAllowedIdentity)) {
            errors.append("Identities allowed by server side of internal")
                .append(" transport ")
                .append(serverAllowedIdentity)
                .append(" are not the same as client side of internal")
                .append(" transport ")
                .append(clientInternalAllowedIdentity)
                .append(".\n");
        }
        final String jeHaAllowedIdentity =
            sp.getTransServerIdentityAllowed(SECURITY_TRANSPORT_JE_HA);
        if (!clientInternalAllowedIdentity.equals(jeHaAllowedIdentity)) {
            errors.append("Identities allowed by JE HA transport ")
                .append(jeHaAllowedIdentity)
                .append(" are not the same as internal transport ")
                .append(clientInternalAllowedIdentity)
                .append(".\n");
        }
        final String clientAllowedIdentity =
            sp.getTransServerIdentityAllowed(SECURITY_TRANSPORT_CLIENT);
        if (!clientInternalAllowedIdentity.equals(clientAllowedIdentity)) {
            errors.append("Identities allowed by client transport ")
                .append(clientAllowedIdentity)
                .append(" are not the same as internal transport ")
                .append(clientInternalAllowedIdentity)
                .append(".\n");
        }

        /* Verify keystore and truststore installation */
        final String result = checkKeystoreInstallation(sp);
        if (result != null) {
            errors.append(result);
        }
        return errors.toString();
    }

    /**
     * Load KeyStore from given key or trust store file.
     * @param storeName KeyStore file name
     * @param storePassword KeyStore password
     * @param storeFlavor the flavor of KeyStore, like keystore that contains
     * private key entries and truststore that contains certificates
     * @param storeType KeyStore type, like jks
     * @return Loaded KeyStore object
     * @throws IllegalArgumentException if any errors occurs during loading
     */
    public static KeyStore loadKeyStore(String storeName,
                                        char[] storePassword,
                                        String storeFlavor,
                                        String storeType)
        throws IllegalArgumentException {

        if (storeType == null || storeType.isEmpty()) {
            storeType = KeyStore.getDefaultType();
        }

        final KeyStore ks;
        try {
            ks = KeyStore.getInstance(storeType);
        } catch (KeyStoreException kse) {
            throw new IllegalArgumentException(
                "Unable to find a " + storeFlavor + " instance of type " +
                    storeType, kse);
        }

        final FileInputStream fis;
        try {
            fis = new FileInputStream(storeName);
        } catch (FileNotFoundException fnfe) {
            throw new IllegalArgumentException(
                "Unable to locate specified " + storeFlavor + " " + storeName,
                fnfe);
        }

        try {
            ks.load(fis, storePassword);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Error reading from " + storeFlavor + " file " + storeName,
                ioe);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalArgumentException(
                "Unable to check " + storeFlavor + " integrity: " + storeName,
                nsae);
        } catch (CertificateException ce) {
            throw new IllegalArgumentException(
                "Not all certificates could be loaded: " + storeName,
                ce);
        } finally {
            try {
                fis.close();
            } catch (IOException ioe) {
                /* ignored */
            }
        }
        return ks;
    }

    /*
     * Check if given transport specify the preferred protocol for client.
     */
    private static String checkClientAllowedProtocols(String trans,
                                                      SecurityParams sp) {
        final String specifiedProtocol = sp.getTransClientAllowProtocols(trans);

        try {
            if (!checkIfProtocolsAllowed(specifiedProtocol)) {
                return "Transport " + trans +
                    " is not using preferred protocols " + specifiedProtocol +
                    " , the prefered protocols are " +
                    PREFERRED_PROTOCOLS_DEFAULT;
            }
        } catch (IllegalArgumentException iae) {
            return "Problem with protocols specified for transport " + trans +
                ": " + iae.getMessage();
        }
        return null;
    }

    /*
     * Check if given protocols are allowed. Given protocols string must be in
     * the format of "x,y,z" using commas as delimiters, and at least one of
     * the specified protocols must be in the preferred protocols list.
     */
    private static boolean checkIfProtocolsAllowed(String protocols) {
        final String[] protocolList = protocols.split(",");

        if (protocolList.length == 0) {
            throw new IllegalArgumentException(
                "'" + protocols + "' does not have the correct format," +
                " must be specified in the format 'x,y,z', using commas as" +
                " delimiters");
        }

        for (String protocol : protocolList) {
            if (preferredProtocols.contains(protocol.trim())) {
                return true;
            }
        }
        return false;
    }

    private static String checkKeystoreInstallation(SecurityParams sp) {
        final KeyStorePasswordSource pwdSrc = KeyStorePasswordSource.create(sp);

        if (pwdSrc == null) {
            /*
             * Return directly, cannot perform following verification
             * without password
             */
            return "Unable to create keystore password source.\n";
        }
        final String keystoreName =
            sp.getConfigDir() + File.separator + sp.getKeystoreFile();
        final String truststoreName =
            sp.getConfigDir() + File.separator + sp.getTruststoreFile();
        final String keyalias =
            sp.getTransServerKeyAlias(SECURITY_TRANSPORT_JE_HA);
        final String allowedIdentities =
            sp.getTransClientIdentityAllowed(SECURITY_TRANSPORT_JE_HA);

        final KeyStore keystore;
        final KeyStore truststore;
        char[] ksPwd = null;

        try {
            ksPwd = pwdSrc.getPassword();
            keystore = loadKeyStore(
                keystoreName, ksPwd, "keystore", sp.getKeystoreType());
            truststore = loadKeyStore(
                truststoreName, ksPwd, "truststore", sp.getTruststoreType());

            /* Check if private key entry exist */
            final PrivateKeyEntry pkEntry = (PrivateKeyEntry)
                keystore.getEntry(keyalias, new PasswordProtection(ksPwd));
            if (pkEntry == null) {

                /*
                 * Return directly, cannot perform following verification
                 * without a private key in key store.
                 */
                return "Private key " + keyalias +
                    " does not exist in the keystore.";
            }

            final Certificate cert = pkEntry.getCertificate();
            if (!(cert instanceof X509Certificate)) {
                /*
                 * Return directly, cannot perform following verification
                 * if private key  in key store does not have valid x509
                 * certificate.
                 */
                return "Certificate of " + keyalias +
                    " is not a valid X509 certificate.\n";
            }

            /*
             * Check if the subject of key is one of the identities allowed.
             * Using regular expression matching to check this as the internal
             * SSL verifier does.
             */
            final X509Certificate x509Cert = (X509Certificate) cert;
            final X500Principal subject = x509Cert.getSubjectX500Principal();
            final String verifyResult =
                verifyCertIdentityAllowed(subject, allowedIdentities);
            if (verifyResult != null) {
                return verifyResult;
            }
            final String issuer = x509Cert.getIssuerX500Principal().getName();

            /* Self-signed certificate, skip following verification */
            if (subject.getName().equals(issuer)) {
                return null;
            }

            final X509CertSelector target = new X509CertSelector();
            final Certificate[] chain = pkEntry.getCertificateChain();
            target.setCertificate((X509Certificate)chain[0]);

            /* Convert certificates array to X509 certificates*/
            final List<X509Certificate> x509Chain =
                new ArrayList<X509Certificate>();

            for (Certificate certificate : chain) {
                if (!(certificate instanceof X509Certificate)) {
                    return "Certificate chain contains invalid " +
                        "X509 certificate " + certificate.toString() + ".\n";
                }
                x509Chain.add((X509Certificate)certificate);
            }
            try {
                final PKIXBuilderParameters params =
                    new PKIXBuilderParameters(keystore, target);
                final CertPathBuilder builder =
                    CertPathBuilder.getInstance("PKIX");

                /*
                 * Disable revocation check for now since we don't have support
                 * to get CRL information, but should be enabled once we can
                 * provide CRL list.
                 */
                params.setRevocationEnabled(false);

                /*
                 * Attempts to build certificate path, if fail,
                 * it will throw a CertPathBuilderException
                 */
                builder.build(params);
            } catch (Exception e) {
                /*
                 * Hide the original exception since the failure of build is
                 * difficult to track down from CertPathBuilderException. The
                 * error message of this exception is not very descriptive, so
                 * here we only warns users the installation is incorrect.
                 */
                return "Problem with verifying certificate chain in keystore " +
                    keystoreName + ".\n";
            }

            /* Check truststore contains necessary certificates */
            boolean foundRequiredTrust = false;
            final Enumeration<String> aliases = truststore.aliases();
            while (aliases.hasMoreElements()) {
                final Certificate trust =
                    truststore.getCertificate(aliases.nextElement());
                if (trust instanceof X509Certificate) {
                    if (x509Cert.equals(trust)) {
                        foundRequiredTrust = true;
                    }
                }
            }
            if (!foundRequiredTrust) {
                return truststoreName +
                    " must contain the certificate " +
                     x509Cert.getSubjectDN().getName() + ".\n";
            }
        } catch (Exception e) {
            return "Unexpected error: " + e.getMessage();
        } finally {
            SecurityUtils.clearPassword(ksPwd);
        }
        return null;
    }

    /**
     * Verify name of X500 principal in RFC1779 format match the regular
     * expression specified in allowedIdentities.
     */
    static String verifyCertIdentityAllowed(X500Principal principal,
                                            String allowedIdentities) {
        final String subjectName = principal.toString();
        final String rfc1779Name = principal.getName(X500Principal.RFC1779);

        if (!checkIdentityAllowed(allowedIdentities, rfc1779Name)) {
            return "The certificate's subject name '" + subjectName +
                "' when displayed in RFC 1779 format as '" + rfc1779Name +
                "' does not match '" + allowedIdentities +
                "' specified in allowedIdentities.\n";
        }
        return null;
    }

    /*
     * Using regular expression matching for now to check if given identity
     * is allowed.
     */
    private static boolean checkIdentityAllowed(String allowedIdentities,
                                                String identity) {
        final String regex = allowedIdentities.substring(
            "dnmatch(".length(), allowedIdentities.length() - 1);
        return identity.matches(regex);
    }

    /*
     * Generate random password in the specified length.
     */
    public static char[] generateKeyStorePassword(int length) {
        final char[] pwd = new char[length];
        for (int i = 0; i < length; i++) {
            pwd[i] = allCharSet.charAt(random.nextInt(allCharSet.length()));
        }
        return pwd;
    }

    /*
     * Generate fixed length of password used for creating a default
     * user of secured KVLite. The fixed length is 12.
     */
    public static char[] generateUserPassword() {
        char[] pwd = new char[12];
        for (int i = 0; i < 3; i++) {
            pwd[i] = upperSet.charAt(random.nextInt(upperSet.length()));
            pwd[i + 3] = lowerSet.charAt(random.nextInt(lowerSet.length()));
            pwd[i + 6] = specialSet.charAt(random.nextInt(specialSet.length()));
            pwd[i + 9] = digitSet.charAt(random.nextInt(digitSet.length()));
        }
        return permuteCharArray(pwd);
    }

    private static char[] permuteCharArray(char[] chars) {
        final List<Character> list = new ArrayList<Character>();
        for (char c : chars) {
            list.add(c);
        }
        Collections.shuffle(list);
        char[] result = new char[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Character c = list.get(i);
            result[i] = c.charValue();
        }
        list.clear();
        return result;
    }
}
