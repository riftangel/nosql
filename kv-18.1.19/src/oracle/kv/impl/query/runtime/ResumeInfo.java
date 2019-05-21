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

package oracle.kv.impl.query.runtime;

import static oracle.kv.impl.util.SerializationUtil.readByteArray;
import static oracle.kv.impl.util.SerializationUtil.writeByteArray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import oracle.kv.impl.api.table.FieldValueImpl;
import oracle.kv.impl.query.QueryStateException;
import oracle.kv.impl.util.FastExternalizable;

/*
 * A query is allowed to run for a limited time only at each server and it will
 * self-terminate even if there more results to be computed. This happens when
 * either a max number of results have been produced or a max number of KBs have
 * been read. In these cases, we say that the query "suspends itself", even
 * though this may not be good terminology because "suspends" may imply that
 * some state remains at the server, which is not true. In fact, all query 
 * state at the server is thrown away; the server just forgets all about the
 * query.
 *
 * If the client wants to get more results from a partition/shard, it resends
 * the query to an appropriate server, which must "resume" the query so that
 * it does not produce any results that have been produced in previous
 * incarnations. In other words, the query must restart where it got suspended.
 *
 * To achieve this, when the query suspends, it collects the info that it will
 * need to resume correctly and sends this info back to the client inside the
 * QueryResult. To resume, the client sends the resume info back to the server,
 * inside the TableQuery op.
 *
 * This class represents the resume info. To summarize, a ResumeInfo is carried
 * from a server to the client inside a QueryResult, and from the client back
 * to the server inside a TableQuery.  
 */
public class ResumeInfo implements FastExternalizable {

    /* Used for tracing only */
    RuntimeControlBlock theRCB;

    /*
     * The number of results received from the server so far. This is needed
     * when a LIMIT clause is pushed to the server. When a server is asked
     * to produce the next result batch, it needs to know how many results
     * it has produced already (in previous batches), so it does not exceed
     * the specified limit. This is necessary when the query is executed
     * at a single partition, because in that case the whole OFFSET/LIMIT
     * clauses are executed at the server. When the query is distributed to
     * multiple partitions/shards, the OFFSET/LIMIT clauses are executed at
     * the client, but a server never needs to send more than OFFSET + LIMIT
     * results. So, a LIMIT equal to the user-specified OFFSET+LIMIT is pushed
     * to the server as an optimization, and numResultsComputed is used as a
     * further optimization. For example, if the batch size is 100 and the
     * server-side limit is 110, this optimization saves the computation and
     * transmission of at least 90 results (110 results with the optimization
     * vs 200 results without). (The savings may be more than 90 because after
     * the client receives one batch, it may immediately ask for the next
     * batch, if its results queue is not full. But with a queue size of 3
     * batches, the maximum savings is 3 batch sizes).
     */
    private int theNumResultsComputed;

    /*
     * It specifies the index range that was being scanned when the query got
     * suspended. This is needed because multiple ranges may be scanned inside
     * the index that is used by a query to access the table.
     */
    private int theCurrentIndexRange;

    private byte[] thePrimResumeKey;

    private byte[] theSecResumeKey;

    /*
     * This is used when the query has a NESTED TABLES clause with descendants
     * and the target table is accessed via a seconadry index. In this case,
     * theDescResumeKey is a primary key that points to the most recently
     * accesed descendant row.
     */
    private byte[] theDescResumeKey;

    /*
     * The moveAfterResumeKey flag is needed to handle SizeLimitExceptions
     * during query processing, and also for NESTED TABLES queries (See
     * ServerTableIter).
     */
    private boolean theMoveAfterResumeKey = true;

    /*
     * Next 3 fields store resume info needed when a query contains
     * a NESTED TABLES clause (see ServerTableIter). 
     */
    private int[] theJoinPathTables;

    private byte[] theJoinPathKey;

    private byte[] theJoinPathSecKey;

    /*
     * theJoinPathMatched is needed when a query contains a NESTED TABLES
     * clause or the query is suspended due to a SizeLimitException (see
     * ServerTableIter). 
     */
    private boolean theJoinPathMatched;

    /*
     * Resume info used for grouping queries. In this case, to know whether
     * a group is finished, we have to evaluate the 1st tuple of the next
     * group. So, if the batch size is N, at the end of a batch, we have
     * at each RN N results plus the 1st tuple of the next group. This extra
     * result must be sent to the client and then back to the server so that
     * the next group is initialized properly.
     */
    private FieldValueImpl[] theGBTuple;

    public ResumeInfo(RuntimeControlBlock rcb) {
        theRCB = rcb;
    }

    void reset() {
        /* don't reset theNumResultsComputed */
        theCurrentIndexRange = 0;
        thePrimResumeKey = null;
        theSecResumeKey = null;
        theDescResumeKey = null;
        theMoveAfterResumeKey = true;
        theJoinPathKey = null;
        theJoinPathSecKey = null;
        theJoinPathTables = null;
        theJoinPathMatched = true;
        theGBTuple = null;
    }

    /*
     * This method is used by a ResumeInfo that lives at the client. It updates
     * the values of "this" with new values coming from the server.
     */
    void refresh(final ResumeInfo src) {

        theNumResultsComputed += src.theNumResultsComputed;

        theCurrentIndexRange = src.theCurrentIndexRange;
        thePrimResumeKey = src.thePrimResumeKey;
        theSecResumeKey = src.theSecResumeKey;
        theDescResumeKey = src.theDescResumeKey;
        theMoveAfterResumeKey = src.theMoveAfterResumeKey;

        theJoinPathTables = src.theJoinPathTables;
        theJoinPathKey = src.theJoinPathKey;
        theJoinPathSecKey = src.theJoinPathSecKey;
        theJoinPathMatched = src.theJoinPathMatched;

        theGBTuple = src.theGBTuple;
    }

    public void setRCB(RuntimeControlBlock rcb) {
        theRCB = rcb;
    }

    public long getNumResultsComputed() {
        return theNumResultsComputed;
    }

    public void setNumResultsComputed(int v) {
        theNumResultsComputed = v;
    }

    public int getCurrentIndexRange() {
        return theCurrentIndexRange;
    }

    public void setCurrentIndexRange(int v) {
        theCurrentIndexRange = v;
    }

    public byte[] getPrimResumeKey() {
        return thePrimResumeKey;
    }

    public void setPrimResumeKey(byte[] resumeKey) {

        if (theRCB != null && theRCB.getTraceLevel() >= 3) {
            theRCB.trace("Setting resume key to\n" +
                         PlanIter.printKey(resumeKey));
        }

        thePrimResumeKey = resumeKey;
    }

    public byte[] getSecResumeKey() {
        return theSecResumeKey;
    }

    public void setSecResumeKey(byte[] resumeKey) {

        if (theRCB != null && theRCB.getTraceLevel() >= 3) {
            theRCB.trace("Setting secondary resume key to\n" +
                         PlanIter.printByteArray(resumeKey));
        }

        theSecResumeKey = resumeKey;
    }

    public byte[] getDescResumeKey() {
        return theDescResumeKey;
    }

    public void setDescResumeKey(byte[] key) {

        if (theRCB != null && theRCB.getTraceLevel() >= 3) {
            theRCB.trace("Setting secondary resume key to\n" +
                         PlanIter.printKey(key));
        }

        theDescResumeKey = key;
    }

    public boolean getMoveAfterResumeKey() {
        return theMoveAfterResumeKey;
    }

    public void setMoveAfterResumeKey(boolean v) {
        theMoveAfterResumeKey = v;
    }

    public int[] getJoinPathTables() {
        return theJoinPathTables;
    }

    public byte[] getJoinPathKey() {
        return theJoinPathKey;
    }

    public byte[] getJoinPathSecKey() {
        return theJoinPathSecKey;
    }

    public boolean getJoinPathMatched() {
        return theJoinPathMatched;
    }

    public void setJoinPathMatched(boolean v) {
        theJoinPathMatched = v;
    }

    public void setJoinPath(
        int[] tables,
        byte[] primKey,
        byte[] idxKey,
        boolean matched) {
        theJoinPathTables = tables;
        theJoinPathKey = primKey;
        theJoinPathSecKey = idxKey;
        theJoinPathMatched = matched;
    }

    public FieldValueImpl[] getGBTuple() {
        return theGBTuple;
    }

    public void setGBTuple(FieldValueImpl[] gbTuple) {
        theGBTuple = gbTuple;
    }

    @Override
    public String toString() {

        StringBuffer sb = new StringBuffer();

        sb.append("theNumResultsComputed = ").append(theNumResultsComputed);
        sb.append("\n");

        sb.append("theCurrentIndexRange = ").append(theCurrentIndexRange);
        sb.append("\n");

        if (thePrimResumeKey != null) {
            sb.append("thePrimResumeKey = ");
            sb.append(PlanIter.printKey(thePrimResumeKey));
            sb.append("\n");
        }

        if (theSecResumeKey != null) {
            sb.append("theSecResumeKey = ");
            sb.append(PlanIter.printByteArray(theSecResumeKey));
            sb.append("\n");
        }

        if (theDescResumeKey != null) {
            sb.append("theDescResumeKey = ");
            sb.append(PlanIter.printKey(theDescResumeKey));
            sb.append("\n");
        }

        if (thePrimResumeKey != null) {
            sb.append("theMoveAfterResumeKey = ").append(theMoveAfterResumeKey);
            sb.append("\n");
        }

        if (theJoinPathKey != null) {
            sb.append("theJoinPathKey = ");
            sb.append(PlanIter.printKey(theJoinPathKey));
            sb.append("\n");

            sb.append("theJoinPathMatched = ").append(theJoinPathMatched);
            sb.append("\n");
        }

        if (theJoinPathSecKey != null) {
            sb.append("theJoinPathSecKey = ");
            sb.append(PlanIter.printByteArray(theJoinPathSecKey));
            sb.append("\n");
        }

        if (theGBTuple != null) {
            sb.append("GB tuple = [ ");
            for (int i = 0; i < theGBTuple.length; ++i) {
                sb.append(theGBTuple[i]).append(" ");
            }
            sb.append("]\n");
        }

        return sb.toString();
    }

    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        out.writeInt(theNumResultsComputed);

        out.writeInt(theCurrentIndexRange);
        writeByteArray(out, thePrimResumeKey);

        if (thePrimResumeKey != null) {
            writeByteArray(out, theSecResumeKey);
            writeByteArray(out, theDescResumeKey);
            out.writeBoolean(theMoveAfterResumeKey);

            PlanIter.serializeIntArray(theJoinPathTables, out, serialVersion);
            PlanIter.serializeByteArray(theJoinPathKey, out, serialVersion);
            PlanIter.serializeByteArray(theJoinPathSecKey, out, serialVersion);
            out.writeBoolean(theJoinPathMatched);
        }

        PlanIter.serializeFieldValues(theGBTuple, out, serialVersion);
    }

    public ResumeInfo(DataInput in, short serialVersion) throws IOException {

        theRCB = null;

        try {
            theNumResultsComputed = in.readInt();

            theCurrentIndexRange = in.readInt();
            thePrimResumeKey = readByteArray(in);

            if (thePrimResumeKey != null) {
                theSecResumeKey = readByteArray(in);
                theDescResumeKey = readByteArray(in);
                theMoveAfterResumeKey = in.readBoolean();

                theJoinPathTables =
                    PlanIter.deserializeIntArray(in, serialVersion);
                theJoinPathKey =
                    PlanIter.deserializeByteArray(in, serialVersion);
                theJoinPathSecKey =
                    PlanIter.deserializeByteArray(in, serialVersion);
                theJoinPathMatched = in.readBoolean();
            }

            theGBTuple = PlanIter.deserializeFieldValues(in, serialVersion);

        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } catch (RuntimeException re) {
            re.printStackTrace();
            throw new QueryStateException(
                "Failed to deserialize ResumeInfo. Reason: " + re);
        }
    }
}
