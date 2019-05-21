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

package oracle.kv.impl.topo;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.kv.impl.util.SerializationUtil.readPackedInt;
import static oracle.kv.impl.util.SerializationUtil.readString;
import static oracle.kv.impl.util.SerializationUtil.writePackedInt;
import static oracle.kv.impl.util.SerializationUtil.writeString;
import static oracle.kv.impl.util.SerialVersion.MASTER_AFFINITY_VERSION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.Topology.Component;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.SerializationUtil;

import com.sleepycat.je.rep.NodeType;
import com.sleepycat.persist.model.Persistent;

import org.codehaus.jackson.node.ObjectNode;

/**
 * The Datacenter topology component.
 *
 * <p>DPL versions: <ul>
 * <li>version 0: original
 * <li>version 1: added repFactor field
 * <li>version 2: added allowArbiters field
 * <li>version 3: added affinityLevel field
 * </ul>
 *
 * @see #writeFastExternal FastExternalizable format
 */
@Persistent(version=3)
public class Datacenter extends Component<DatacenterId> {

    private static final long serialVersionUID = 1L;

    /*
     * Convert master affinity or non-master affinity to an integer value.
     * The value is 10 when master affinity is true, and the value is 1
     * when master affinity is false.
     */
    public static final int MASTER_AFFINITY = 10;
    public static final int NO_MASTER_AFFINITY = 1;

    /** Data centers with version=1 are of type PRIMARY by default. */
    private static final DatacenterType DEFAULT_DATACENTER_TYPE =
        DatacenterType.PRIMARY;

    private String name;
    private int repFactor;
    private boolean allowArbiters;

    /*
     * Add integer affinityLevel as the property to determine whether datacenter
     * can host master RNs or not. And in the user-side, the datacenter has
     * boolean masterAffinity to determine whether datacenter can host master
     * RNs or not. So, all boolean affinity passed into this class will be
     * converted as integer affinity. And the class has two methods to convert
     * boolean affinity to integer affinity and convert integer affinity to
     * boolean affinity.
     *
     * The hope is that using an integer affinityLevel in this class would
     * allow use to avoid needing to upgrade the class if we wanted to allow
     * users to set integer affinity values instead of boolean ones.
     *
     * The default affinityLevel is NO_MASTER_AFFINITY_LEVEL
     */
    private int affinityLevel;

    /** Creates a new Datacenter. */
    public static Datacenter newInstance(final String name,
                                         final int repFactor,
                                         final DatacenterType datacenterType,
                                         final boolean allowArbiters,
                                         final boolean masterAffinity) {

        checkNull("datacenterType", datacenterType);
        return new DatacenterV2(name,
                                repFactor,
                                datacenterType,
                                allowArbiters,
                                masterAffinity);
    }

    /**
     * Reads a Datacenter instance from the input stream.
     */
    static Datacenter readFastExternal(Topology topology,
                                       DatacenterId datacenterId,
                                       DataInput in,
                                       short serialVersion)
        throws IOException {

        return new DatacenterV2(topology, datacenterId, in, serialVersion);
    }

    private Datacenter(String name, int repFactor, boolean allowArbiters,
                       boolean masterAffinity) {
        this.name = name;
        this.repFactor = repFactor;
        this.allowArbiters = allowArbiters;
        int minRepFactor = 1;
        if (allowArbiters) {
            minRepFactor = 0;
        }
        if (repFactor < minRepFactor) {
            throw new IllegalArgumentException(
                "Replication factor must be greater than or equal to " +
                minRepFactor);
        }
        affinityLevel = masterConvertToLevel(masterAffinity);
    }

    private Datacenter(Datacenter datacenter) {
        super(datacenter);
        name = datacenter.name;
        repFactor = datacenter.repFactor;
        allowArbiters = datacenter.allowArbiters;
        affinityLevel = datacenter.affinityLevel;
    }

    private Datacenter() {
    }

    private Datacenter(Topology topology,
                       DatacenterId datacenterId,
                       DataInput in,
                       short serialVersion)
        throws IOException {

        super(topology, datacenterId, in, serialVersion);
        name = readString(in, serialVersion);
        repFactor = readPackedInt(in);
        allowArbiters = in.readBoolean();

        /*
         * Datacenter has no master affinity when serialVersion is less than
         * WRITE_ZONE_AFFINITY_VERSION.
         **/
        if (serialVersion < MASTER_AFFINITY_VERSION) {
            affinityLevel = NO_MASTER_AFFINITY;
        } else {
            affinityLevel = readPackedInt(in);
        }
    }

    /**
     * Set the default value for the affinityLevel as NO_MASTER_AFFINITY_LEVEL.
     */
    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        /*
         * Set NO_MASTER_AFFINITY_LEVEL as default value for affinityLevel,
         * when the read affinityLevel is 0 (it is the default value for an
         * uninitialized integer)
         */
        if (affinityLevel == 0) {
            affinityLevel = NO_MASTER_AFFINITY;
        }
    }

    /**
     * Convert master affinity or non-master affinity to affinity level. The
     * value of affinity level is MASTER_AFFINITY_LEVEL when master affinity
     * is true, and the value of affinity level is NO_MASTER_AFFINITY_LEVEL
     * when master affinity is false.
     *
     * @param masterAffinity
     * @return affinity level
     */
    private int masterConvertToLevel(boolean masterAffinity) {
        if (masterAffinity) {
            return MASTER_AFFINITY;
        }
        return NO_MASTER_AFFINITY;
    }

    /**
     * Convert affinity level to master affinity or non-master affinity. Set
     * master affinity as true when affinity level is 10 or above, or set master
     * affinity as false.
     *
     * @param level
     * @return master affinity
     */
    private boolean levelConvertToMaster(int level) {
        if (level >= MASTER_AFFINITY) {
            return true;
        }
        return false;
    }

    /**
     * Writes this object to the output stream.  Format:
     * <ol>
     * <li> ({@link Component}) {@code super}
     * <li> ({@link SerializationUtil#writeString String}) {@link #getName
     *      name}
     * <li> ({@link SerializationUtil#writePackedInt packedInt}) {@link
     *      #getRepFactor repFactor}
     * <li> ({@link DataOutput#writeBoolean boolean}) {@link #getAllowArbiters
     *      allowArbiters}
     * <li> ({@link DatacenterType}) {@link #getDatacenterType datacenterType}
     * </ol>
     *
     * Note that the output format always includes the DatacenterType, and the
     * instance creating when reading will always be (at least) DatacenterV2.
     */
    @Override
    public void writeFastExternal(DataOutput out, short serialVersion)
        throws IOException {

        super.writeFastExternal(out, serialVersion);
        writeString(out, serialVersion, name);
        writePackedInt(out, repFactor);
        out.writeBoolean(allowArbiters);

        /* Write masterAffinity only after master affinity is introduced */
        if (serialVersion >= MASTER_AFFINITY_VERSION) {
            writePackedInt(out, affinityLevel);
        }
        getDatacenterType().writeFastExternal(out, serialVersion);
    }

    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#getResourceType()
     */
    @Override
    public ResourceType getResourceType() {
        return ResourceType.DATACENTER;
    }

    /* Returns the name associated with the Datacenter. */
    public String getName() {
        return name;
    }

    public int getRepFactor() {
        return repFactor;
    }

    /* repfactor is excluded from the hash code because it's mutable. */
    public void setRepFactor(int factor) {
        repFactor = factor;
    }

    /**
     * Returns the type of the data center.
     */
    public DatacenterType getDatacenterType() {
        return DEFAULT_DATACENTER_TYPE;
    }

    public boolean getAllowArbiters() {
        return allowArbiters;
    }

    public boolean getMasterAffinity() {
        return levelConvertToMaster(affinityLevel);
    }


    /* (non-Javadoc)
     * @see oracle.kv.impl.topo.Topology.Component#clone()
     */
    @Override
    public Datacenter clone() {
        return new Datacenter(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + getDatacenterType().hashCode();
        result = prime * result + getRepFactor();
        result = prime * result + (getAllowArbiters() ? 0 : 1);
        result = prime * result + affinityLevel;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof Datacenter)) {
            return false;
        }
        final Datacenter other = (Datacenter) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }

        if (repFactor == other.repFactor &&
            getDatacenterType().equals(other.getDatacenterType()) &&
            allowArbiters == other.allowArbiters &&
            affinityLevel == other.affinityLevel) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("id=" + getResourceId() + " name=" + name +
                  " repFactor=" + repFactor + " type=" + getDatacenterType() +
                  " allowArbiters=" + getAllowArbiters() +
                  " masterAffinity=" + getMasterAffinity());
        return sb.toString();
    }

    @Override
    public ObjectNode toJson() {
        final ObjectNode top = JsonUtils.createObjectNode();
        top.put("resourceId", getResourceId().toString());
        top.put("name", name);
        top.put("repFactor", repFactor);
        top.put("type", getDatacenterType().toString());
        top.put("allowArbiters", getAllowArbiters());
        top.put("masterAffinity", getMasterAffinity());
        return top;
    }

    /**
     * A nested class of Datacenter that contains methods that should only be
     * called on the server side.  This class is not included in the
     * client-side JAR file, which also means that the JE NodeType class is not
     * needed there.
     */
    public static class ServerUtil {

        /**
         * Returns the default rep node type for a datacenter.
         *
         * <p>Under normal circumstances, all of the RNs in a datacenter will
         * use this value as their node type.  Administrators can override the
         * node type to temporarily change the node type for a set of RNs as
         * part of disaster recovery procedures.  [#23447]
         *
         * @param dc the datacenter
         * @return the default rep node type
         */
        public static NodeType getDefaultRepNodeType(final Datacenter dc) {
            return getDefaultRepNodeType(dc.getDatacenterType());
        }

        /**
         * Returns the default rep node type for the specified datacenter type.
         *
         * @param type the datacenter type
         * @return the default rep node type
         */
        public static NodeType getDefaultRepNodeType(
            final DatacenterType type) {

            switch (type) {
            case PRIMARY:
                return NodeType.ELECTABLE;
            case SECONDARY:
                return NodeType.SECONDARY;
            default:
                throw new AssertionError();
            }
        }
    }

    /**
     * Define a subclass of Datacenter for instances with a non-default value
     * for the DatacenterType.
     */
    @Persistent
    private static class DatacenterV2 extends Datacenter {
        private static final long serialVersionUID = 1L;
        private DatacenterType datacenterType;

        DatacenterV2(final String name,
                     final int repFactor,
                     final DatacenterType datacenterType,
                     final boolean allowArbiters,
                     final boolean masterAffinity) {
            super(name, repFactor, allowArbiters, masterAffinity);
            checkNull("datacenterType", datacenterType);
            this.datacenterType = datacenterType;
        }

        private DatacenterV2(final DatacenterV2 datacenter) {
            super(datacenter);
            datacenterType = datacenter.datacenterType;
        }

        private DatacenterV2(Topology topology,
                             DatacenterId datacenterId,
                             DataInput in,
                             short serialVersion)
            throws IOException {

            super(topology, datacenterId, in, serialVersion);
            datacenterType =
                DatacenterType.readFastExternal(in, serialVersion);
        }

        /** For DPL */
        @SuppressWarnings("unused")
        private DatacenterV2() {
        }

        @Override
        public DatacenterType getDatacenterType() {
            return datacenterType;
        }

        /* (non-Javadoc)
         * @see oracle.kv.impl.topo.Topology.Component#clone()
         */
        @Override
        public DatacenterV2 clone() {
            return new DatacenterV2(this);
        }
    }
}
