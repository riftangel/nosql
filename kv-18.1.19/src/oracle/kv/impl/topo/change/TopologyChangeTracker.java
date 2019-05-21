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

package oracle.kv.impl.topo.change;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.Topology.Component;

import com.sleepycat.persist.model.Persistent;

/**
 * Manages all changes associated with the Topology. The changes are maintained
 * as a sequence of Add, Update, Remove log records. The changes are noted
 * by the corresponding log methods which are required to be invoked in
 * a logically consistent order. For example, a RepGroup must be added to the
 * topology, before a RepNode belonging to it is added to the group.
 * <p>
 * The Add and Remove records reference components that could potentially be in
 * active use in the Topology.This is done to keep the in-memory representation
 * of the Topology as compact as possible.
 */
@Persistent
public class TopologyChangeTracker implements Serializable {

    private static final long serialVersionUID = 1L;

    /* The topology associated with the changes. */
    private Topology topology;

    private int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    /*
     * The ordered list of changes, with the most recent change at the end of
     * the list.
     */
    private LinkedList<TopologyChange> changes;

    public TopologyChangeTracker(Topology topology) {
        this.topology = topology;
        changes = new LinkedList<TopologyChange>();
    }

    @SuppressWarnings("unused")
    private TopologyChangeTracker() {
    }

    /**
     * Returns the first sequence number in the list of changes or -1
     * if the change list is empty.
     */
    public int getFirstChangeSeqNum() {

        return (changes.size() == 0) ?
            - 1 : changes.get(0).getSequenceNumber();
    }

    /**
     * Returns the current sequence number.
     *
     * The sequence number is incremented with each logical change to the
     * topology. For example, add a RG, add a RN, change an RN, etc. A Topology
     * that is out of date, with an earlier sequence number, can be updated by
     * applying the changes that it is missing, in sequence, up to the target
     * sequence number.
     */
    public int getSeqNum() {
        return seqNum;
    }

    /* Methods used to log changes. */

    public void logAdd(Component<?> component) {
        component.setSequenceNumber(++seqNum);
        changes.add(new Add(seqNum, component));
    }

    public void logUpdate(Component<?> newComponent) {
        newComponent.setSequenceNumber(++seqNum);
        changes.add(new Update(seqNum, newComponent));
    }

    public void logRemove(ResourceId resourceId) {

        changes.add(new Remove(++seqNum, resourceId));
        assert topology.get(resourceId) == null;
    }

    /**
     * @see Topology#getChanges(int)
     */
    public List<TopologyChange> getChanges(int startSeqNum) {

        int minSeqNum = (changes.size() == 0) ? 0 :
                         changes.getFirst().sequenceNumber;

        if (startSeqNum < minSeqNum) {
            return null;
        }

        if (startSeqNum > changes.getLast().getSequenceNumber()) {
            return null;
        }

        LinkedList<TopologyChange> copy = new LinkedList<TopologyChange>();
        for (TopologyChange change : changes) {
            if (change.getSequenceNumber() >= startSeqNum) {
                copy.add(change.clone());
            }
        }
        return copy;
    }

    /**
     * @see Topology#getChanges(int)
     */
    public  List<TopologyChange> getChanges() {
        return getChanges((changes.size() == 0) ? 0 :
                          changes.getFirst().sequenceNumber);
    }

    /**
     * @see Topology#discardChanges(int)
     */
    public void discardChanges(int startSeqNum) {

        for (Iterator<TopologyChange> i = changes.iterator();
             i.hasNext() && (i.next().getSequenceNumber() <= startSeqNum);) {
            i.remove();
        }
    }
}
