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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.async.DialogContext;

/**
 * Captures the performance metrics of a dialog.
 */
public class DialogPerf {

    private final DialogContext context;

    private final AtomicInteger numOutputsWritten = new AtomicInteger();
    private final AtomicInteger numInputsRead = new AtomicInteger();
    private final AtomicInteger numFramesSent = new AtomicInteger();
    private final AtomicInteger numFramesRecv = new AtomicInteger();

    private final long initTimeMs = System.currentTimeMillis();
    private final long initNs = System.nanoTime();
    private volatile long doneNs = 0;

    private volatile List<Event> events = null;
    private volatile List<Long> timesNs = null;
    /**
     * Events for a dialog.
     */
    public enum Event {
        WRITE,
        READ,
        SEND,
        RECV,
        FIN,
        ABORT,
    }

    public DialogPerf(DialogContext context) {
        this.context = context;
    }

    /**
     * Marks the associated dialog as sampled and starts sampling for the
     * dialog.
     */
    public synchronized void startSampling() {
        if (events == null) {
            events = new ArrayList<Event>();
            timesNs = new ArrayList<Long>();
        }
    }

    public boolean isSampled() {
        return (timesNs != null);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append("initTimeMs=").append(initTimeMs);
        sb.append(" latencyMs=").append(
                String.format("%.2f", getLatencyNs() / 1e6));
        sb.append(" nwrite=").append(numOutputsWritten.get());
        sb.append(" nread=").append(numInputsRead.get());
        sb.append(" nsend=").append(numFramesSent.get());
        sb.append(" nrecv=").append(numFramesRecv.get());
        sb.append(" detailed=").append(getDetailedTimeString(false));
        sb.append("}");
        return sb.toString();
    }

    /**
     * Called when a dialog event happens.
     */
    public void onEvent(Event evt) {
        switch (evt) {
        case WRITE:
            numOutputsWritten.incrementAndGet();
            break;
        case READ:
            numInputsRead.incrementAndGet();
            break;
        case SEND:
            numFramesSent.incrementAndGet();
            break;
        case RECV:
            numFramesRecv.incrementAndGet();
            break;
        case FIN:
        case ABORT:
            doneNs = System.nanoTime();
            break;
        default:
            throw new IllegalStateException(
                    String.format("Unknown event for dialog: %s)", evt));
        }
        if (timesNs != null) {
            synchronized(this) {
                events.add(evt);
                if ((evt == Event.FIN) || (evt == Event.ABORT)) {
                    timesNs.add(doneNs);
                } else {
                    timesNs.add(System.nanoTime());
                }
            }
        }
    }

    /**
     * Returns the number of MessageOutput written.
     */
    public int getNumOutputsWritten() {
        return numOutputsWritten.get();
    }

    /**
     * Returns the number of MessageInput read.
     */
    public int getNumInputsRead() {
        return numInputsRead.get();
    }

    /**
     * Returns the number of frames sent.
     */
    public int getNumFramesSent() {
        return numFramesSent.get();
    }

    /**
     * Returns the number of frames received.
     */
    public int getNumFramesRecv() {
        return numFramesRecv.get();
    }

    /**
     * Returns the latency.
     */
    public long getLatencyNs() {
        long latency = doneNs - initNs;
        if (latency < 0) {
            return latency;
        }
        return latency;
    }

    /**
     * Returns detailed time string.
     */
    public String getDetailedTimeString(boolean addContextId) {
        final StringBuilder sb = new StringBuilder();
        if (addContextId) {
            sb.append(String.format("%s:%x",
                        Long.toString(context.getDialogId(), 16),
                        context.getConnectionId()));
        }
        sb.append("(").append(initTimeMs).append(")");
        if (timesNs != null) {
            synchronized(this) {
                sb.append("[");
                for (int i = 0; i < events.size(); ++i) {
                    sb.append(events.get(i)).
                        append("(").
                        append(String.format(
                                    "%.2f", (timesNs.get(i) - initNs) / 1e6)).
                        append(")").append(" ");
                }
                sb.append("ms]");
            }
        }
        return sb.toString();
    }
}
