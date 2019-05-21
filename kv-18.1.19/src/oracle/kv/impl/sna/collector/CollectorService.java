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

package oracle.kv.impl.sna.collector;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.util.sklogger.SkLogger;

/**
 * SN collector service that will start all agents to collect monitor data.
 * Currently, two agents are registered and started. Agents are the active
 * entities that fetch metric data at prescribed times. The two current agents
 * are the JMX agent, which reads the jmx mbean periodically, and the ping
 * agent, which calls ping on the running service to determine health.
 *
 * Agents funnel collected data and collector recorders, which route and
 * possibly transform the data and send it to a destination. The defacto
 * recorder is the FileRecorder, which logs this information in a format that
 * that can be consumed by monitoring systems. It is also possible to add
 * a custom destination by specifying an optional collector recorder as a
 * dynamically loaded class.
 */
public class CollectorService {

    private final List<CollectorAgent> agents;
    private final CollectorRecorder fileRecorder;
    private final AtomicReference<CollectorRecorder> optionalRecorder;
    private boolean shutdown;
    private final GlobalParamsListener globalParamsListener;
    private final SNParamsListener snParamsListener;
    private final Logger snaLogger;

    /*
     * Keep a reference to the snp and gp because they would be needed as
     * parameters to an optional recorder
     */
    private StorageNodeParams storageNodeParams;
    private GlobalParams globalParams;

    public CollectorService(StorageNodeParams snp,
                            GlobalParams gp,
                            SecurityParams sp,
                            LoginManager loginManager,
                            Logger snaLogger) {
        this.snaLogger = snaLogger;
        this.globalParams = gp;
        this.storageNodeParams = snp;

        /*
         * Create all recorders. These are the modules that take collected
         * info and transform and transport them to a destination.
         * - the fileRecorder is mandatory
         * - an additional recorder may be dynamically loaded and created
         */
        fileRecorder = new FileCollectorRecorder(snp, gp);
        optionalRecorder = new AtomicReference<>();
        setOptionalRecorder(snp, gp);

        /* Register all agents */
        agents = new ArrayList<CollectorAgent>();
        PingCollectorAgent pingAgent =
            new PingCollectorAgent(gp, snp, loginManager, fileRecorder,
                                   snaLogger);

        JMXCollectorAgent metricAgent =
             new JMXCollectorAgent(snp, sp, fileRecorder, optionalRecorder,
                                   snaLogger);
        agents.add(pingAgent);
        agents.add(metricAgent);

        if (gp.getCollectorEnabled()) {
            for (CollectorAgent agent : agents) {
                agent.start();
            }
        }
        shutdown = false;
        globalParamsListener = new GlobalParamsListener();
        snParamsListener = new SNParamsListener();
    }

    /**
     * Dynamically load and create the specified collector recorder.
     */
    private void setOptionalRecorder(StorageNodeParams snp, GlobalParams gp) {


        String recorderClassName = gp.getCollectorRecorder();
        if (recorderClassName == null) {
            optionalRecorder.set(null);
            return;
        }


        String classname = recorderClassName.trim();
        if ((classname == null) || (classname.length() == 0)) {
            optionalRecorder.set(null);
            return;
        }

        /*
         * An optional recorder was specified in the params, but there's already
         * an existing one.
         */
        CollectorRecorder current = optionalRecorder.get();
        if (current != null &&
            current.getClass().getName().equals(classname)) {
            return;
        }

        /* An optional recorder was specified, and it's different */
        Class<?> cl;
        try {
            cl = Class.forName(classname);
            Class<? extends CollectorRecorder> recorderClass =
                cl.asSubclass(CollectorRecorder.class);
            Constructor<? extends CollectorRecorder> ctor =
                recorderClass.getDeclaredConstructor
                (StorageNodeParams.class,
                 GlobalParams.class,
                 SkLogger.class);
            SkLogger useLogger = null;
            if (snaLogger != null) {
                useLogger = new SkLogger(snaLogger);
            }
            CollectorRecorder recorder = ctor.newInstance(snp, gp, useLogger);
            /*
             * Note: currently only JMXCollectorAgent use the optional
             * recorder, so old recorder will be closed in JMXCollectorAgent.
             * When there are multiple Agents using the optional recorder,
             * we might consider closing the old recorder here instead.
             */
            optionalRecorder.set(recorder);
        } catch (Exception e) {
            snaLogger.severe("Couldn't create " + classname + " " + e);
        }
    }

    public synchronized void updateParams(GlobalParams newGlobalParams,
                                          StorageNodeParams newSNParams) {
        if (shutdown) {
            /* Collector service has been shutdown, don't update parameters. */
            return;
        }
        if (newGlobalParams != null) {
            snaLogger.fine("Collector service: new global params: " +
                newGlobalParams.getMap().toString());
            globalParams = newGlobalParams;
        }
        if (newSNParams != null) {
            snaLogger.fine("Collector service: new SN params: " +
                newSNParams.getMap().toString());
            storageNodeParams = newSNParams;
        }

        /* Is there a new optional recorder? */
        setOptionalRecorder(storageNodeParams, globalParams);

        for (CollectorAgent agent : agents) {
            agent.updateParams(newGlobalParams, newSNParams);
        }
    }

    public synchronized void shutdown() {
        shutdown = true;
        for (CollectorAgent agent : agents) {
            agent.stop();
        }

        /*
         * Shutdown recorders after agents are closed, since agents use
         * recorders
         */
        fileRecorder.close();
        CollectorRecorder recorder = optionalRecorder.get();
        if (recorder != null) {
            recorder.close();
        }
    }

    public ParameterListener getGlobalParamsListener() {
        return globalParamsListener;
    }

    public ParameterListener getSNParamsListener() {
        return snParamsListener;
    }

    /**
     * Global parameter change listener.
     */
    private class GlobalParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            GlobalParams newGlobalParams = new GlobalParams(newMap);
            updateParams(newGlobalParams, null /* newSNParams */);
        }
    }

    /**
     * StorageNode parameter change listener.
     */
    private class SNParamsListener implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            StorageNodeParams newSNParams = new StorageNodeParams(newMap);
            updateParams(null /* newGlobalParams */, newSNParams);
        }
    }
}
