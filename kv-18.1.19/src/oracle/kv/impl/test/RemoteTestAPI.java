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

import oracle.kv.impl.util.registry.RemoteAPI;

/**
 * RemoteTestAPI is used conditionally in order to extend a service's
 * interface to include test-only functions.  Implementations of this interface
 * should live in test packages and be conditionally instantiated using
 * reflection, e.g.:
 * try {
 *   Class cl = Class.forName("ImplementsRemoteTestInterface");
 *   Constructor<?> c = cl.getConstructor(getClass());
 *   RemoteTestAPI rti = (RemoteTestAPI) c.newInstance(this);
 *   rti.start();
 * } catch (Exception ignored) {}
 *
 * Most implementations should extend RemoteTestBase which is implemented in
 * the test tree.
 */
public class RemoteTestAPI extends RemoteAPI {

    private final RemoteTestInterface remote;

    private RemoteTestAPI(RemoteTestInterface remote)
        throws RemoteException {

        super(remote);
        this.remote = remote;
    }

    public static RemoteTestAPI wrap(RemoteTestInterface remote)
        throws RemoteException {

        return new RemoteTestAPI(remote);
    }

    /**
     * Start the service. This call should create a binding.
     */
    public void start()
        throws RemoteException {

        remote.start(getSerialVersion());
    }

    /**
     * Stop the service.  This call should unbind the object.
     */
    public void stop()
        throws RemoteException {

        remote.stop(getSerialVersion());
    }

    /**
     * Tell the service to exit (calls System.exit(status)).  Callers should
     * expect this to always throw an exception because the service's process
     * will be unable to reply.
     *
     * @param status The exit status to use.
     */
    public void processExit(int status)
        throws RemoteException {

        remote.processExit(status, getSerialVersion());
    }

    /**
     * Tell the service to halt (calls Runtime.getRuntime.halt(status)).  This
     * differs from exit in that shutdown hooks are not run. Callers should
     * expect this to always throw an exception because the service's process
     * will be unable to reply.
     *
     * @param status The exit status to use.
     */
    public void processHalt(int status)
        throws RemoteException {

        remote.processHalt(status, getSerialVersion());
    }

    /**
     * Tell the service to invalidate it's JE environment, returning whether
     * invalidation was performed successfully.
     */
    public boolean processInvalidateEnvironment(boolean corrupted)
        throws RemoteException {

        return
            remote.processInvalidateEnvironment(corrupted, getSerialVersion());
    }

    /**
     * Ask the implementation to set a TestHook.
     *
     * @param hook The TestHook instance to use.
     *
     * @param memberName The name of the member in the target class that holds
     * the TestHook instance.
     */
    public void setHook(TestHook<?> hook, String memberName)
        throws RemoteException {

        remote.setHook(hook, memberName, getSerialVersion());
    }

    public void setTTLTime(long time)
        throws RemoteException {
        remote.setTTLTime(time, getSerialVersion());
    }

    /**
     * Add arguments to the command line for the service with the specified
     * name.  The storage node will add the arguments just for when the service
     * is next started or restarted.  This command only has an effect when
     * called on an SN.
     *
     * @param serviceName the name of the service
     * @param args the arguments to add for the named service
     */
    public void addServiceExecArgs(String serviceName, String... args)
        throws RemoteException {

        remote.addServiceExecArgs(serviceName, args, getSerialVersion());
    }
}
