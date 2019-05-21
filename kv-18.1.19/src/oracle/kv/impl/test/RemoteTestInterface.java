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

package oracle.kv.impl.test;

import java.rmi.RemoteException;

import oracle.kv.impl.util.registry.VersionedRemote;

/**
 * RemoteTestInterface is used conditionally in order to extend a service's
 * interface to include test-only functions.  Implementations of this interface
 * should live in test packages and be conditionally instantiated using
 * reflection, e.g.:
 * try {
 *   Class cl = Class.forName("ImplementsRemoteTestInterface");
 *   Constructor<?> c = cl.getConstructor(getClass());
 *   RemoteTestInterface rti = (RemoteTestInterface) c.newInstance(this);
 *   rti.start();
 * } catch (Exception ignored) {}
 *
 * Most implementations should extend RemoteTestBase which is implemented in
 * the test tree.
 */
public interface RemoteTestInterface extends VersionedRemote {

    /**
     * Start the service. This call should create a binding.
     */
    public void start(short serialVersion)
        throws RemoteException;

    /**
     * Stop the service.  This call should unbind the object.
     */
    public void stop(short serialVersion)
        throws RemoteException;

    /**
     * Tell the service to exit (calls System.exit(status)).  Callers should
     * expect this to always throw an exception because the service's process
     * will be unable to reply.
     *
     * @param status The exit status to use.
     */
    public void processExit(int status, short serialVersion)
        throws RemoteException;

    /**
     * Add arguments to the command line for the service with the specified
     * name.  The storage node will add the arguments just for when the service
     * is next started or restarted.  This command only has an effect when
     * called on an SN.
     *
     * @param serviceName the name of the service
     * @param args the arguments to add for the named service
     */
    public void addServiceExecArgs(String serviceName,
                                   String[] args,
                                   short serialVersion)
        throws RemoteException;

    /**
     * Tell the service to halt (calls Runtime.getRuntime.halt(status)).  This
     * differs from exit in that shutdown hooks are not run. Callers should
     * expect this to always throw an exception because the service's process
     * will be unable to reply.
     *
     * @param status The exit status to use.
     */
    public void processHalt(int status, short serialVersion)
        throws RemoteException;

    /**
     * Tell the service to invalidate it's JE environment, returning whether
     * the invalidation was performed successfully.
     */
    public boolean processInvalidateEnvironment(boolean corrupted,
                                                short serialVersion)
        throws RemoteException;

    /**
     * Ask the implementation to set a TestHook.
     *
     * @param hook The TestHook instance to use.
     *
     * @param memberName The name of the member in the target class that holds
     * the TestHook instance.
     */
    public void setHook(TestHook<?> hook,
                        String memberName,
                        short serialVersion)
        throws RemoteException;

    /**
     * Asks the implementation to set the TTL time to the specified value.
     * This is used by TTL tests to set a RepNode's TTL time to allow timeouts
     * to occur in short periods of real time.
     *
     * @param time the absolute time to use for the TTL time. A value of 0
     * removes the test hook, restoring normal TTL time on the RN.
     * 0.
     */
    public void setTTLTime(long time, short serialVersion)
        throws RemoteException;
}
