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

package oracle.kv.impl.async;

import oracle.kv.impl.api.AsyncRequestHandler;
import oracle.kv.impl.async.registry.ServiceRegistry;

/**
 * The standard dialog type families for asynchronous service interfaces.
 */
public enum StandardDialogTypeFamily implements DialogTypeFamily {

    /** Dialog type family for {@link ServiceRegistry}. */
    SERVICE_REGISTRY(0),

    /** Dialog type family for {@link AsyncRequestHandler}. */
    ASYNC_REQUEST_HANDLER(1);

    /**
     * The bootstrap dialog type used for the service registry, which has a
     * known dialog type ID.
     */
    public static final DialogType SERVICE_REGISTRY_DIALOG_TYPE =
        new DialogType(0, SERVICE_REGISTRY);

    private StandardDialogTypeFamily(int ordinal) {
        if (ordinal != ordinal()) {
            throw new IllegalArgumentException("Wrong ordinal");
        }
        DialogType.registerTypeFamily(this);
    }

    @Override
    public int getFamilyId() {
        return ordinal();
    }

    @Override
    public String getFamilyName() {
        return name();
    }

    @Override
    public String toString() {
        return name() + '(' + ordinal() + ')';
    }
}
