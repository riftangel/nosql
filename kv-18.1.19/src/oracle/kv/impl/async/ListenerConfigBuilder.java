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

import java.util.HashMap;
import java.util.Map;


/**
 * A builder to build the listener configuration.
 */
public class ListenerConfigBuilder {

    /**
     * Allow overriding defaults by setting a system property with the
     * specified prefix followed by the option name.
     */
    private static final String DEFAULT_OVERRIDE_PROPERTY_PREFIX =
        "oracle.kv.async.listener.config.default.";

    private static final Map<AsyncOption<?>, Object> optionDefaults =
        new HashMap<AsyncOption<?>, Object>();

    static {
        optionDefault(AsyncOption.SO_RCVBUF, null);
        optionDefault(AsyncOption.SO_REUSEADDR, true);
        optionDefault(AsyncOption.SSO_BACKLOG, 0);
    }

    private ListenerPortRange portRange;
    private final Map<AsyncOption<?>, Object> options =
        new HashMap<AsyncOption<?>, Object>();
    private EndpointConfigBuilder endpointConfigBuilder =
        new EndpointConfigBuilder();

    /**
     * Creates an instance of this class.
     */
    public ListenerConfigBuilder() {
    }

    /**
     * Sets the port range.
     *
     * @param range the port range
     * @return this builder
     */
    public ListenerConfigBuilder portRange(ListenerPortRange range) {
        portRange = range;
        return this;
    }

    /**
     * Sets the listener option.
     *
     * @param option the option
     * @param value the value
     * @return this builder
     */
    public <T>
        ListenerConfigBuilder option(AsyncOption<T> option, T value) {

        options.put(option, value);
        return this;
    }

    /**
     * Sets the builder for endpoint configuration.
     *
     * @param builder the endpoint configuration builder
     * @return this builder
     */
    public ListenerConfigBuilder
        endpointConfigBuilder(EndpointConfigBuilder builder) {

        endpointConfigBuilder = builder;
        return this;
    }

    /**
     * Builds the configuration.
     */
    public ListenerConfig build() {
        return new ListenerConfig(portRange, options, optionDefaults,
                endpointConfigBuilder.build());
    }

    /**
     * Sets the listener option default value.
     */
    private static <T> void optionDefault(AsyncOption<T> option, T value) {
        final String propertyOverride = System.getProperty(
            DEFAULT_OVERRIDE_PROPERTY_PREFIX + option.name());
        if (propertyOverride != null) {
            value = option.parseStringValue(propertyOverride);
        }
        optionDefaults.put(option, value);
    }

}
