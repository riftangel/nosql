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

package oracle.kv.impl.admin;

import static oracle.kv.impl.param.ParameterState.RN_NODE_TYPE;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.admin.VerifyConfiguration.CompareParamsResult;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.sna.StorageNodeAgentAPI;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

import com.sleepycat.je.rep.NodeType;

public class SnConsistencyUtils {

    /**
     * Checks the global and SN parameters.
     */
    static ParamCheckResults checkParameters(StorageNodeAgentAPI sna,
                                             StorageNodeId snId,
                                             Parameters params)
       throws RemoteException {

        /* Get SN configuration parameters */
        final LoadParameters configParams;
        configParams = sna.getParams();
        ParamCheckResults retVal = new ParamCheckResults();

        CompareParamsResult snCompare =
            VerifyConfiguration.compareParams(
                configParams,
                params.getGlobalParams().getMap());
        if (snCompare != CompareParamsResult.NO_DIFFS) {
            retVal.setGlobalDiff();
        }

        /* check sn parameters */
        ParameterMap snParams = null;
        for (StorageNodeParams tSNp : params.getStorageNodeParams()) {
            if (tSNp.getStorageNodeId().equals(snId)) {
                snParams = tSNp.getMap();
                break;
            }
        }
        snCompare =
            VerifyConfiguration.compareParams(configParams,
                                              snParams);

        if (snCompare != CompareParamsResult.NO_DIFFS) {
            retVal.addDiff(snId);
        }

        for (RepNodeParams rnDbParams : params.getRepNodeParams()) {

            /*
             * Compare sn config with admin db for given RN
             */
            if (!rnDbParams.getStorageNodeId().equals(snId)) {
                continue;
            }
            snCompare =
                VerifyConfiguration.compareParams(configParams,
                                                  rnDbParams.getMap());

            if (snCompare == CompareParamsResult.NO_DIFFS) {
                continue;
            }

            final RepNodeId rnId = rnDbParams.getRepNodeId();

            if (snCompare == CompareParamsResult.MISSING) {
                retVal.addMissing(rnId);
            } else {
                ParameterMap rnCfg =
                    configParams.getMap(rnId.getFullName(),
                                        ParameterState.REPNODE_TYPE);
                NodeType nt =
                    NodeType.valueOf(
                        rnCfg.getOrDefault(RN_NODE_TYPE).asString());
                /*
                 * Do not attempt to fix a RN with an out of sync
                 * node type. Leave this correction to plan repair.
                 */
                if (rnDbParams.getNodeType() == nt) {
                    retVal.addDiff(rnId);
                }
            }
        }
        return retVal;
    }

    /**
     * Used to save results of parameter consistency between the
     * Admin database and the corresponding SN configuration.
     *
     */
    public static class ParamCheckResults {
        private boolean globalParametersDiff;
        private final List<ResourceId> idsDiff;
        private final List<ResourceId> idsMissing;

        private ParamCheckResults() {
            idsDiff = new ArrayList<ResourceId>();
            idsMissing = new ArrayList<ResourceId>();
        }

        private void addDiff(ResourceId resId) {
            idsDiff.add(resId);
        }

        private void addMissing(ResourceId resId) {
            idsMissing.add(resId);
        }

        private void setGlobalDiff() {
            globalParametersDiff = true;
        }

        public boolean getGlobalDiff() {
            return globalParametersDiff;
        }

        public List<ResourceId> getDiffs() {
            return idsDiff;
        }

        public List<ResourceId> getMissing() {
            return idsMissing;
        }
    }
}