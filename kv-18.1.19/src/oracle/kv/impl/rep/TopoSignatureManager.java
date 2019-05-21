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
package oracle.kv.impl.rep;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.security.ExecutionContext;
import oracle.kv.impl.security.InvalidSignatureException;
import oracle.kv.impl.security.SignatureHelper;
import oracle.kv.impl.security.SignatureFaultException;
import oracle.kv.impl.security.SystemPrivilege;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.server.LoggerUtils.SecurityLevel;

public class TopoSignatureManager
    implements TopologyManager.PostUpdateListener,
               TopologyManager.PreUpdateListener {

    /* Topology signature helper, cloud be null when security is not enabled */
    private final SignatureHelper<Topology> topoSignatureHelper;
    private final Logger logger;

    public TopoSignatureManager(SignatureHelper<Topology> topoSignatureHelper,
                                Logger logger) {
        this.topoSignatureHelper = topoSignatureHelper;
        this.logger = logger;
    }

    @Override
    public void preUpdate(Topology topology)
        throws InvalidSignatureException {

        if (isInternalUpdater()) {
            return;
        }

        if (topoSignatureHelper != null && !verifyTopology(topology)) {
            throw new InvalidSignatureException(
                "Invalid signature for topology with seq# " +
                    topology.getSequenceNumber());
        }
    }

    @Override
    public boolean postUpdate(Topology topology) {

        if (topoSignatureHelper != null && topology.getSignature() == null) {
            signTopology(topology);
        }

        /* Keeps this listener */
        return false;
    }

    /**
     * Checks whether the current updater is an internal component.
     *
     * @return true if security is disabled or current user has INTLOPER
     * privilege
     */
    private boolean isInternalUpdater() {
        if (ExecutionContext.getCurrent() == null) {
            return true;
        }
        return ExecutionContext.getCurrentPrivileges().implies(
                    SystemPrivilege.INTLOPER);
    }

    private void signTopology(Topology topo) {
        try{
            final byte[] sigBytes = topoSignatureHelper.sign(topo);
            topo.updateSignature(sigBytes);
            logger.log(Level.INFO, // TODO: to be fine
                       "Updated signature for topology seq# {0}",
                       topo.getSequenceNumber());
        } catch (SignatureFaultException sfe) {
            logger.log(
                Level.WARNING,
                "Failed to generate signature for topology of seq# {0} for {1}",
                new Object[] { topo.getSequenceNumber(), sfe });
        }
    }

    private boolean verifyTopology(Topology topo) {

        final byte[] sigBytes = topo.getSignature();

        if (sigBytes == null || sigBytes.length == 0) {
            logger.log(
                SecurityLevel.SEC_WARNING,
                "Empty signature. Verification failed for topology seq# {0}",
                topo.getSequenceNumber());

            return false;
        }

        try {
            final boolean passedCheck =
                topoSignatureHelper.verify(topo, sigBytes);
            logger.log(
                (passedCheck ? Level.INFO : SecurityLevel.SEC_WARNING),
                "Signature verification {0} for topology with seq# {1}",
                new Object[] { (passedCheck ? "passed" : "failed"),
                               topo.getSequenceNumber() });

            return passedCheck;
        } catch (SignatureFaultException sfe) {
            logger.log(
                Level.WARNING,
                "Problem verifying signature for topology with seq# {0}: {1}",
                new Object[] {topo.getSequenceNumber(), sfe});
        }

        return false;
    }
}
