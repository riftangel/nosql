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
import java.util.List;

import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.monitor.Tracker;
import oracle.kv.impl.monitor.ViewListener;
import oracle.kv.impl.topo.ResourceId;

/**
 * Tracks plan state changes.  The UI uses this to dynamically update a listing
 * of plan states.
 */
public class PlanStateChangeTracker
    extends Tracker<PlanStateChange> implements ViewListener<PlanStateChange> {

    private final static int PRUNE_FREQUENCY = 40;

    private final List<EventHolder<PlanStateChange>> queue;
    private int newInfoCounter = 0;

    public PlanStateChangeTracker() {
        super();
        queue = new ArrayList<EventHolder<PlanStateChange>>();
    }

    private void prune() {
        long interesting = getEarliestInterestingTimeStamp();

        while (!queue.isEmpty()) {
            EventHolder<PlanStateChange> psc = queue.get(0);
            if (psc.getSyntheticTimestamp() > interesting) {
                /* Stop if we've reached the earliest interesting timestamp. */
                break;
            }
            queue.remove(0);
        }
    }

    @Override
    public void newInfo(ResourceId rId, PlanStateChange psc) {
        synchronized (this) {
            if (newInfoCounter++ % PRUNE_FREQUENCY == 0) {
                prune();
            }

            long syntheticTimestamp = getSyntheticTimestamp(psc.getTime());

            queue.add
                (new EventHolder<PlanStateChange>
                 (syntheticTimestamp, psc,
                 false /* Plans states are never recordable. */));
        }
        notifyListeners();
    }

    /**
     * Get a list of events that have occurred since the given time.
     */
    @Override
    public synchronized
        RetrievedEvents<PlanStateChange> retrieveNewEvents(long since) {

        List<EventHolder<PlanStateChange>> values =
            new ArrayList<EventHolder<PlanStateChange>>();

        long syntheticStampOfLastRecord = since;
        for (EventHolder<PlanStateChange> psc : queue) {
            if (psc.getSyntheticTimestamp() > since) {
                values.add(psc);
                syntheticStampOfLastRecord = psc.getSyntheticTimestamp();
            }
        }

        return new RetrievedEvents<PlanStateChange>(syntheticStampOfLastRecord,
                                                    values);
    }
}
