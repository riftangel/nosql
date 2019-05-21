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

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.util.HashMap;
import java.util.Map;

/**
 * The configuration of a listener.
 */
public class ListenerConfig {

    private final ListenerPortRange portRange;
    private final Map<AsyncOption<?>, Object> options =
        new HashMap<AsyncOption<?>, Object>();
    private final Map<AsyncOption<?>, Object> optionDefaults =
        new HashMap<AsyncOption<?>, Object>();
    private final EndpointConfig endpointConfig;

    ListenerConfig(ListenerPortRange portRange,
                   Map<AsyncOption<?>, Object> options,
                   Map<AsyncOption<?>, Object> optionDefaults,
                   EndpointConfig endpointConfig) {
        checkNull("portRange", portRange);
        checkNull("options", options);
        checkNull("optionDefaults", optionDefaults);
        checkNull("endpointConfig", endpointConfig);
        this.portRange = portRange;
        this.options.putAll(options);
        this.optionDefaults.putAll(optionDefaults);
        this.endpointConfig = endpointConfig;
    }

    /**
     * Gets the port range.
     *
     * @return the port range.
     */
    public ListenerPortRange getPortRange() {
        return portRange;
    }

    /**
     * Gets the listener option value.
     *
     * @return the listener option
     */
    public <T> T getOption(AsyncOption<T> option) {
        if (!optionDefaults.containsKey(option)) {
            throw new IllegalArgumentException(
                    String.format(
                        "Not a suitable option for listener: %s", option));
        }
        Object value = options.get(option);
        if (value == null) {
            value = optionDefaults.get(option);
        }
        return option.type().cast(value);
    }

    /**
     * Gets endpoint configuration for accepted a connection.
     *
     * @return the endpoint configuration
     */
    public EndpointConfig getEndpointConfig() {
        return endpointConfig;
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof ListenerConfig)) {
            return false;
        }

        ListenerConfig that = (ListenerConfig) obj;
        return (this.portRange.equals(that.portRange) &&
                this.options.equals(that.options) &&
                this.optionDefaults.equals(that.optionDefaults) &&
                this.endpointConfig.equals(that.endpointConfig));
    }


    @Override
    public int hashCode() {
        int prime = 31;
        int hash = portRange.hashCode();
        hash = prime * hash + options.hashCode();
        hash = prime * hash + optionDefaults.hashCode();
        hash = prime * hash + endpointConfig.hashCode();
        return hash;
    }
}
