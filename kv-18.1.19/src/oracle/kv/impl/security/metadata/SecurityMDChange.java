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

package oracle.kv.impl.security.metadata;

import java.io.Serializable;

import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElement;
import oracle.kv.impl.security.metadata.SecurityMetadata.SecurityElementType;

/**
 * Class for recording a change of SecurityMetadata on its elements. The change
 * includes three types of operation so far: ADD, UPDATE and REMOVE. The type
 * of element operated on will also be recorded.
 */
public abstract class SecurityMDChange implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public enum SecurityMDChangeType { ADD, UPDATE, REMOVE }

    protected int seqNum = Metadata.EMPTY_SEQUENCE_NUMBER;

    SecurityMDChange(final int seqNum) {
        this.seqNum = seqNum;
    }

    /*
     * Ctor with deferred sequence number setting.
     */
    private SecurityMDChange() {
    }

    /**
     * Get the sequence number of the security metadata change.
     */
    public int getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(final int seqNum) {
        this.seqNum = seqNum;
    }

    /**
     * Get the type of element involved in the change
     */
    public abstract SecurityElementType getElementType();

    public abstract SecurityMDChangeType getChangeType();

    public abstract SecurityElement getElement();

    abstract String getElementId();

    @Override
    public abstract SecurityMDChange clone();

    /**
     * The change of UPDATE.
     */
    public static class Update extends SecurityMDChange {

        private static final long serialVersionUID = 1L;

        SecurityElement element;

        public Update(final SecurityElement element) {
            this.element = element;
        }

        private Update(final int seqNum, final SecurityElement element) {
            super(seqNum);
            this.element = element;
        }

        @Override
        public SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.UPDATE;
        }

        @Override
        public SecurityElement getElement() {
            return element;
        }

        @Override
        String getElementId() {
            return element.getElementId();
        }

        @Override
        public SecurityElementType getElementType() {
            return element.getElementType();
        }

        @Override
        public Update clone() {
            return new Update(seqNum, element.clone());
        }
    }

    /**
     * The change of ADD. It extends the CHANGE class since it shares almost
     * the same behavior and codes of CHANGE except the ChangeType.
     */
    public static class Add extends Update {

        private static final long serialVersionUID = 1L;

        public Add(final SecurityElement element) {
            super(element);
        }

        private Add(final int seqNum, final SecurityElement element) {
            super(seqNum, element);
        }

        @Override
        public SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.ADD;
        }

        @Override
        public Add clone() {
            return new Add(seqNum, element.clone());
        }
    }

    /**
     * The change of REMOVE.
     */
    public static class Remove extends SecurityMDChange {

        private static final long serialVersionUID = 1L;

        private String elementId;
        private SecurityElement element;
        private SecurityElementType elementType;

        public Remove(final String removedId,
                      final SecurityElementType eType,
                      final SecurityElement element) {
            this.elementId = removedId;
            this.elementType = eType;
            this.element = element;
        }

        private Remove(final int seqNum,
                       final String removedId,
                       final SecurityElementType eType,
                       final SecurityElement element) {
            super(seqNum);
            this.elementId = removedId;
            this.elementType = eType;
            this.element = element;
        }

        @Override
        public SecurityMDChangeType getChangeType() {
            return SecurityMDChangeType.REMOVE;
        }

        @Override
        public SecurityElement getElement() {
            return element;
        }

        @Override
        String getElementId() {
            return elementId;
        }

        @Override
        public SecurityElementType getElementType() {
            return elementType;
        }

        @Override
        public Remove clone() {
            return new Remove(seqNum, elementId, elementType, element);
        }
    }
}
