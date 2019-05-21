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

package oracle.kv.impl.pubsub;

import java.util.concurrent.atomic.AtomicLong;

import oracle.kv.impl.pubsub.security.StreamClientAuthHandler;

import com.sleepycat.je.utilint.VLSN;

/**
 * Object represents the statistics of the replication stream consumer
 */
public class ReplicationStreamConsumerStat {

    /* parent consumer */
    private final ReplicationStreamConsumer parent;

    /* last streamed VLSN */
    private volatile VLSN lastStreamVLSN;
    /* the start VLSN requested by client */
    private volatile VLSN reqStartVLSN;
    /* the start VLSN acked by Feeder */
    private volatile VLSN ackedStartVLSN;
    /* the last time receiving a msg from feeder */
    private volatile long lastMsgTimeMs;

    /*- counters -*/
    /* number of puts */
    private final AtomicLong numPuts;
    /* number of dels */
    private final AtomicLong numDels;
    /* number of commits */
    private final AtomicLong numCommits;
    /* number of aborts */
    private final AtomicLong numAborts;
    /* number of exceptions */
    private final AtomicLong numExceptions;
    /* number of successful reconnect */
    private final AtomicLong numSuccReconn;

    /* last committed vlsn before reconnect, internally used only */
    private volatile VLSN lastVLSNBeforeReconnect;

    ReplicationStreamConsumerStat(ReplicationStreamConsumer parent) {
        this.parent = parent;
        reqStartVLSN = VLSN.NULL_VLSN;
        ackedStartVLSN = VLSN.NULL_VLSN;
        lastVLSNBeforeReconnect = VLSN.NULL_VLSN;
        lastMsgTimeMs = 0;

        numPuts = new AtomicLong(0);
        numDels = new AtomicLong(0);
        numCommits = new AtomicLong(0);
        numAborts = new AtomicLong(0);
        numExceptions = new AtomicLong(0);
        numSuccReconn = new AtomicLong(0);
    }

    /**
     * Gets the start VLSN requested by client
     *
     * @return start VLSN requested by client
     */
    VLSN getReqStartVLSN() {
        return reqStartVLSN;
    }

    /**
     * Gets the start VLSN acked by server
     *
     * @return start VLSN acked by server
     */
    VLSN getAckedStartVLSN() {
        return ackedStartVLSN;
    }

    /**
     * Gets the VLSN of last committed transaction
     *
     * @return last commit VLSN
     */
    VLSN getLastCommitVLSN() {
        return parent.getTxnBuffer().getLastCommitVLSN();
    }

    /**
     * Gets VLSN of last aborted transaction
     *
     * @return last aborted VLSN
     */
    VLSN getLastAbortVLSN() {
        return parent.getTxnBuffer().getLastAbortVLSN();
    }

    /**
     * Gets num of open txns
     *
     * @return num of open txns
     */
    long getOpenTxns() {
        return parent.getTxnBuffer().getOpenTxns();
    }

    /**
     * Gets num of committed txns
     *
     * @return num of committed txns
     */
    long getCommitTxns() {
        return parent.getTxnBuffer().getCommitTxns();
    }

    /**
     * Gets num of committed ops
     *
     * @return num of committed ops
     */
    long getCommitOps() {
        return parent.getTxnBuffer().getCommitOps();
    }

    /**
     * Gets num of abort txns
     *
     * @return num of abort txns
     */
    long getAbortTxns() {
        return parent.getTxnBuffer().getAbortTxns();
    }

    /**
     * Gets num of abort ops
     *
     * @return num of abort ops
     */
    long getAbortOps() {
        return parent.getTxnBuffer().getAbortOps();
    }

    /**
     * Gets the last time the consumer received a msg from feeder
     *
     * @return the last time the consumer received a msg from feeder
     */
    long getLastMsgTimeMs() {
        return lastMsgTimeMs;
    }

    /**
     * Gets the last streamed VLSN
     *
     * @return the last streamed VLSN
     */
    VLSN getLastStreamedVLSN() {
        return lastStreamVLSN;
    }

    /**
     * Sets the requested start vlsn
     *
     * @param vlsn  the requested start vlsn
     */
    void setReqStartVLSN(VLSN vlsn) {
        reqStartVLSN = vlsn;
    }

    /**
     * Sets the acked start vlsn
     *
     * @param vlsn the acked start vlsn
     */
    void setAckedStartVLSN(VLSN vlsn) {
        ackedStartVLSN = vlsn;
    }

    /**
     * Sets the ast time the consumer received a msg from feeder
     *
     * @param t timestamp of last msg
     */
    void setLastMsgTimeMs(long t) {
        lastMsgTimeMs = t;
    }

    /**
     * Sets the number of successful re-connections
     *
     * @param t  number of successful re-connections
     */
    void setNumSuccReconn(long t) {
        numSuccReconn.set(t);
    }

    /* getters and increments for counters */

    long getNumPuts() {
        return numPuts.get();
    }

    long getNumDels() {
        return numDels.get();
    }

    long getNumCommits() {
        return numCommits.get();
    }

    long getNumAborts() {
        return numAborts.get();
    }

    long getNumExceptions() {
        return numExceptions.get();
    }

    long getNumSuccReconn() {
        return numSuccReconn.get();
    }

    VLSN getLastVLSNBeforeReconnect() {
        return lastVLSNBeforeReconnect;
    }

    void incrNumPuts(VLSN vlsn) {
        numPuts.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumDels(VLSN vlsn) {
        numDels.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumCommits(VLSN vlsn) {
        numCommits.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumAborts(VLSN vlsn) {
        numAborts.incrementAndGet();
        lastStreamVLSN = vlsn;
    }

    void incrNumExceptions() {
        numExceptions.incrementAndGet();
    }

    void setLastVLSNBeforeReconnect(VLSN vlsn) {
        lastVLSNBeforeReconnect = vlsn;
    }

    /**
    * Gets the number of times that token has been refreshed
     *
     * @return the number of times that token has been refreshed
     */
     long getNumTokenRefreshed() {
         if (parent.getAuthHandler() == null) {
             return 0;
         }
         return ((StreamClientAuthHandler)parent.getAuthHandler())
             .getNumTokenRefreshed();
     }

    /**
     * Gets max num of open txns
     *
     * @return max num of open txns
     */
    private long getMaxOpenTxns(){
        return parent.getTxnBuffer().getMaxOpenTxns();
    }

    String dumpStat() {
        return "\nStatistics of RSC " + parent.getConsumerId() + ":\n" +
               "requested start VLSN: " + reqStartVLSN + "\n" +
               "acknowledged start VLSN: " + ackedStartVLSN + "\n" +
               "# committed ops: " + getCommitOps() + "\n" +
               "# aborted ops: " + getAbortOps() + "\n" +
               "# committed txns: " + getCommitTxns() + "\n" +
               "# aborted txns: " + getAbortTxns() + "\n" +
               "# open txns: " + getOpenTxns() + "\n" +
               "# max open txns: " + getMaxOpenTxns() + "\n" +
               "# successful reconnections: " + getNumSuccReconn() + "\n" +
               "# token refreshed: " + getNumTokenRefreshed() + "\n" +
               "last committed txn vlsn: " + getLastCommitVLSN() + "\n" +
               "last aborted txn vlsn: " + getLastAbortVLSN();
    }

}
