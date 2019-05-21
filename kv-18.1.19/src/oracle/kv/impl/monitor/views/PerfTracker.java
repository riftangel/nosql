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

package oracle.kv.impl.monitor.views;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.util.server.LoggerUtils;

/**
 *
 */
public class PerfTracker extends Tracker<PerfEvent>
	implements ViewListener<PerfEvent> {

    private final static int CHUNK_SIZE = 15;

    /* Keep status information for each resource. */
    private final Map<ResourceId, PerfEvent> resourcePerf;
    private final Logger perfFileLogger;
    private int headerCounter;
    private final List<EventHolder<PerfEvent>> queue;

    public PerfTracker(AdminServiceParams params) {
        super();
        resourcePerf =  new ConcurrentHashMap<ResourceId, PerfEvent>();
        perfFileLogger =
            LoggerUtils.getPerfFileLogger(this.getClass(),
                                          params.getGlobalParams(),
                                          params.getStorageNodeParams());
        headerCounter = 1;
        queue = new ArrayList<EventHolder<PerfEvent>>();
    }

    private void prune() {
        long interesting =
        	getEarliestInterestingTimeStamp();

        while (!queue.isEmpty()) {
            EventHolder<PerfEvent> pe = queue.get(0);
            if (pe.getSyntheticTimestamp() > interesting) {
                /* Stop if we've reached the earliest interesting timestamp. */
                break;
            }
            queue.remove(0);
        }
    }

    @Override
    public void newInfo(ResourceId rId, PerfEvent p) {

        synchronized (this) {
            /* Print a header, and prune the queue, every now and then. */
            if (--headerCounter == 0) {
                headerCounter = CHUNK_SIZE;
                perfFileLogger.info(PerfEvent.HEADER);
                prune();
            }

            /* log into the perf stat file. */
            perfFileLogger.info(p.getColumnFormatted());

            /* Save in a map, for later perusal by the UI. */
            resourcePerf.put(rId, p);
            long syntheticTimestamp = getSyntheticTimestamp(p.getChangeTime());
            queue.add(new EventHolder<PerfEvent>
                      (syntheticTimestamp, p,
                       p.needsAlert())); /* alertable == recordable. */
        }
        notifyListeners();
    }

    /**
     * Get the current performance for all resources, for display.
     */
    public Map<ResourceId, PerfEvent> getPerf() {
        return new HashMap<ResourceId, PerfEvent>(resourcePerf);
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    public synchronized
        RetrievedEvents<PerfEvent> retrieveNewEvents(long pointInTime) {

        List<EventHolder<PerfEvent>> values = 
            new ArrayList<EventHolder<PerfEvent>>();

        long syntheticStampOfLastRecord = pointInTime;
        for (EventHolder<PerfEvent> pe : queue) {
            if (pe.getSyntheticTimestamp() > pointInTime) {
                values.add(pe);
                syntheticStampOfLastRecord = pe.getSyntheticTimestamp();
            }
        }

        return
            new RetrievedEvents<PerfEvent>(syntheticStampOfLastRecord, values);
    }
}
