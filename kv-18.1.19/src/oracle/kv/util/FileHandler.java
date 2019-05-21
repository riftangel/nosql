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

package oracle.kv.util;

import java.io.IOException;
import java.util.logging.Level;

import oracle.kv.impl.util.server.LoggerUtils;

/**
 * KVStore instances of java.util.logging.Logger are configured to use this
 * implementation of java.util.logging.ConsoleHandler. By default, the
 * handler's level is {@link Level#INFO}. To enable the console output, use the
 * standard java.util.logging.LogManager configuration to set the desired
 * level:
 * <pre>
 * oracle.kv.util.FileHandler.level=FINE
 * </pre>
 */
public class FileHandler extends java.util.logging.FileHandler {

    int limit;
    int count;

    /*
     * Using a KV specific handler lets us enable and disable output for
     * all kvstore component.
     */
    public FileHandler(String pattern, int limit, int count, boolean append)
        throws IOException, SecurityException {

        super(pattern, limit, count, append);

        this.limit = limit;
        this.count = count;
        Level level = null;
        String propertyName = getClass().getName() + ".level";

        String levelProperty = LoggerUtils.getLoggerProperty(propertyName);
        if (levelProperty == null) {
            level = Level.ALL;
        } else {
            level = Level.parse(levelProperty);
        }

        setLevel(level);
    }

    public int getCount() {
        return count;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * When a handler is closed, we want to remove it from any associated
     * loggers, so that a handler will be created again if the logger is
     * ressurected.
     */
    @Override
    public synchronized void close() {
        super.close();
        LoggerUtils.removeHandler(this);
    }
}

