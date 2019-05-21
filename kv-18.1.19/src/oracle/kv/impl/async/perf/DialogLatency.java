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

package oracle.kv.impl.async.perf;

import com.sleepycat.utilint.Latency;
import com.sleepycat.utilint.LatencyStat;

/**
 * A perf entry that shows the dialog latency stats.
 */
public class DialogLatency {

    /*
     * TODO: make these configurable, maybe also adapt to max latency, or maybe
     * use another way to compute latency stats (e.g., some streaming
     * algorithm, instead of histogram).
     */
    private static final long MAX_LATENCY_MS_DEFAULT = 1000;

    private final String name;
    private final LatencyStat latencyStat =
        new LatencyStat(MAX_LATENCY_MS_DEFAULT);

    public DialogLatency(String name) {
        this.name = name;
    }

    public void update(DialogPerf dperf) {
        latencyStat.set(dperf.getLatencyNs());
    }

    public void reset() {
        latencyStat.clear();
    }

    public String get() {
        Latency stat = latencyStat.calculate();
        String result = String.format("[%s]stat=(%s)", name, stat);
        reset();
        return result;
    }
}
