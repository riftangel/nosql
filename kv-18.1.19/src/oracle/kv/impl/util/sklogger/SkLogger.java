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

package oracle.kv.impl.util.sklogger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.contextlogger.ContextFormatter;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.contextlogger.ContextLogManager.WithLogContext;
import oracle.kv.impl.util.sklogger.MetricFamilySamples.Sample;

import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

/**
 * SkLogger will be used to log {@link Metric}, {@link StringEvent} and
 * normal log in a format that is known for Monitor System.
 * <pre>
 * SkLogger configuration example:
 * # define levels for skloggers
 * oracle.nosql.sc.api.AutomationService.level=ALL
 * oracle.nosql.sc.api.TMService.level=INFO
 * # define handlers for all skloggers
 * oracle.nosql.sc.skhandlers=SkFileHandler,SkConsoleHandler
 * # define Handler properties
 * SkFileHandler.dir=/tmp
 * SkFileHandler.limit=5000000
 * SkFileHandler.count=5
 * SkFileHandler.level=ALL
 * SkFileHandler.append=true
 * SkConsoleHandler.level=ALL
 * </pre>
 * <ul>
 * <li>The format for Metrics is:</li>
 * <pre>
 * [DateTime] METRIC [componentId] [JSON String of all metric fields]
 *
 * For example:
 * 2017-07-10 08:24:00.042 METRIC hostname1 {"proxy_opType":"LIST_TABLES", ... }
 * </pre>
 * <li>The format for Event is:</li>
 * <pre>
 * [DateTime] EVENT [componentId] [EVENT_NAME] [EVENT_LEVEL] [EVENT_SUBJECT]
 * [EVENT_MESSAGE]
 * [EVENT_MESSAGE]
 * [EVENT_MESSAGE]

 * For example:
 * 2017-07-10 08:24:03.310 EVENT hostname10 TableUtils FINE table ddl error
 *      Error: at (1, 0) mismatched input 'adrop' expecting...
 *      rule stack: [parse, statement]
 *      ...
 * </pre>
 * <li>The format for normal log is:</li>
 * <pre>
 * [DateTime] [LOG_LEVEL] [componentId] [LOG_MESSAGE]
 * [LOG_MESSAGE]
 * [LOG_MESSAGE]
 *
 * For example:
 * 2017-07-10 08:23:48.609 INFO hostname1 User Event Triggered: UpgradeEvent
 * more message
 * more message
 * </pre>
 * </ul>
 */
public class SkLogger implements MetricProcessor, StringEventProcessor {

    /**
     * System environment variable name for componentId
     * TODO this constant should belong to all, not only SkLogger. But at
     * this moment, only SkLogger care about componentId.
     */
    public static final String COMPONENTID_ENV = "componentId";

    private static final String LINE_SEPARATOR =
        System.getProperty("line.separator");

    // New SkLogger configuration properties

    /* to set which Handlers to use for SkLoggers */
    private static final String SK_HANDLERS = ".skhandlers";
    /* to use FileHandle if it is set to .skhandlers property */
    private static final String SK_FILEHANDLER = "skfilehandler";
    /* to use ConsoleHandle if it is set to .skhandlers property */
    private static final String SK_CONSOLEHANDLER = "skconsolehandler";
    /* to set FileHandler directory */
    private static final String SK_FILEHANDLER_DIR = "SkFileHandler.dir";
    /* to set FileHandler count */
    private static final String SK_FILEHANDLER_COUNT = "SkFileHandler.count";
    /* to set FileHandler limit */
    private static final String SK_FILEHANDLER_LIMIT = "SkFileHandler.limit";
    /* to set FileHandler append */
    private static final String SK_FILEHANDLER_APPEND = "SkFileHandler.append";
    /* to set FileHandler level */
    private static final String SK_FILEHANDLER_LEVEL = "SkFileHandler.level";
    /* to set ConsoleHandler level */
    private static final String SK_CONSOLEHANDLER_LEVEL =
        "SkConsoleHandler.level";

    /*
     * Logger need share FileHandler that logs to the same file.
     */
    private static final ConcurrentHashMap<String, FileHandler>
        FILE_HANDLER_MAP = new ConcurrentHashMap<String, FileHandler>();
    /*
     * Logger name and file name key/value map. It is used to check if there is
     * a loggerName with different fileName.
     */
    private static final ConcurrentHashMap<String, String>
        LOG_FILE_MAP = new ConcurrentHashMap<String, String>();

    private final ConcurrentHashMap<String, String> properties =
        new ConcurrentHashMap<String, String>();
    private Logger logger;
    /*
     * The min level value of all Handlers, it can be used to do pre-check
     * before executing an expensive log message constructing.
     */
    private int minHandlerLevel = Level.OFF.intValue();

    public SkLogger(Logger logger) {
        this.logger = logger;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        final Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                minHandlerLevel = Math.min(h.getLevel().intValue(),
                                           minHandlerLevel);
            }
        }
    }

    /**
     * Wrap a {@link java.util.logging.Logger} named {loggerName}.{componentId}
     * and set SkLogger handlers from log configuration file.
     * Set componentId to mark the source of log for Monitor system.
     * Set logger file name to componentId + ".log" if FileHandler is
     * configured.
     */
    public SkLogger(String loggerName, String componentId) {
        this(loggerName, componentId, componentId + ".log");
    }

    /**
     * Wrap a {@link java.util.logging.Logger} named {loggerName}.{componentId}.
     * If useSkConfig, set SkLogger handlers from log configuration file and
     * then set logger file name to componentId + ".log" if FileHandler is
     * configured.
     * Set componentId to mark the source of log for Monitor system.
     */
    public SkLogger(String loggerName,
                    String componentId,
                    boolean useSkConfig) {
        logger = getLogger(loggerName, componentId);
        if (useSkConfig) {
            addHandlers(componentId, componentId + ".log");
        }
    }

    /**
     * Wrap a {@link java.util.logging.Logger} named {loggerName}.{componentId}
     * and set SkLogger handlers from log configuration file.
     * Set componentId to mark the source of log for Monitor system.
     * Set logger file name to fileName if FileHandler is configured.
     */
    public SkLogger(String loggerName, String componentId, String fileName) {
        logger = getLogger(loggerName, componentId);
        addHandlers(componentId, fileName);
    }

    private static Logger getLogger(String loggerName, String componentId) {
        /*
         * loggerName concat componentId to be the real logger name so that
         * it can support different componentId for the same loggerName.
         * As Logger name is dot-separated name, replace the "." to "_" in
         * componentId.
         */
        final String name = loggerName + "." + componentId.replace('.', '_');
        final Logger logger = Logger.getLogger(name);
        logger.setUseParentHandlers(false);
        return logger;
    }

    /**
     * Get a SkLogger and add a ConsoleHandler to it by default. This is
     * mainly used for test.
     */
    public static SkLogger getSkLoggerWithConsole(String loggerName,
                                                  String componentId) {
        final SkLogger sklogger = new SkLogger(loggerName, componentId, false);
        final ConsoleHandler handler = new ConsoleHandler(componentId,
                                                          Level.ALL);
        sklogger.addHandler(handler);
        return sklogger;
    }

    /**
     * Log the collected metricFamily from registered metrics, and they will be
     * collected by Monitor system.
     */
    @Override
    public void process(MetricFamilySamples<?> metricFamily) {
        if (metricFamily == null) {
            return;
        }
        if (!isLoggable(MonitorLevel.METRIC)) {
            return;
        }
        final String name = metricFamily.getName();
        final List<String> labelNames = metricFamily.getLabelNames();
        for (Sample<?> sample : metricFamily.getSamples()) {
            final ObjectNode jsonRoot = createJsonHeader();

            for (int i = 0; i < labelNames.size(); i++) {
                final String fullName = name + StatsData.DELIMITER +
                                        labelNames.get(i);
                final String labelValue = sample.labelValues.get(i);
                jsonRoot.put(fullName, labelValue);
            }

            for (Entry<String, Object> entry :
                sample.dataValue.toMap().entrySet()) {

                final String key = entry.getKey();
                String fullName;
                if (key.isEmpty()) {
                    fullName = name;
                } else {
                    fullName = name + StatsData.DELIMITER + entry.getKey();
                }

                final Object dataValue = entry.getValue();
                //TODO is there a better way?
                if (dataValue instanceof Double) {
                    final Double val = (Double) dataValue;
                    if (Double.isNaN(val) ||
                        Double.isInfinite(val)) {
                        jsonRoot.put(fullName, 0.0);
                    } else {
                        jsonRoot.put(fullName, val);
                    }
                } else if (dataValue instanceof Float) {
                    final Float val = (Float) dataValue;
                    if (Float.isNaN(val) ||
                        Float.isInfinite(val)) {
                        jsonRoot.put(fullName, 0.0);
                    } else {
                        jsonRoot.put(fullName, val);
                    }
                } else if (dataValue instanceof Long) {
                    jsonRoot.put(fullName, (Long) dataValue);
                } else if (dataValue instanceof Integer) {
                    jsonRoot.put(fullName, (Integer) dataValue);
                } else if (dataValue instanceof Boolean) {
                    jsonRoot.put(fullName, (Boolean) dataValue);
                }  else {
                    jsonRoot.put(fullName, entry.getValue().toString());
                }
            }
            final ObjectWriter writer = JsonUtils.createWriter(false);
            try {
                final StringBuilder sb = new StringBuilder();
                sb.append(writer.writeValueAsString(jsonRoot));
                final LogRecord record =
                    new LogRecord(MonitorLevel.METRIC,
                                  sb.toString());
                record.setMillis(metricFamily.getReportTimeMs());
                logger.log(record);
            } catch (IOException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    /**
     * Log the event and it will be collected by Monitor system.
     */
    @Override
    public void process(StringEvent event) {
        if (event == null) {
            return;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(event.getStatsName());
        sb.append(" " + event.getLevel().getLocalizedName());
        sb.append(" " + event.getSubject());
        if (event.getMessage() != null) {
            sb.append(LINE_SEPARATOR + event.getMessage());
        }
        final MonitorLevel level =
            MonitorLevel.getEventLevel(event.getLevel().intValue());
        final LogRecord record = new LogRecord(level, sb.toString());
        record.setMillis(event.getReportTimeMs());
        record.setThrown(event.getThrown());
        logger.log(record);
    }

    /**
     * Set common key/value property for all metrics.
     */
    public SkLogger setProperty(String key, String value) {
        properties.put(key, value);
        return this;
    }

    private ObjectNode createJsonHeader() {
        final ObjectNode jsonRoot = JsonUtils.createObjectNode();
        if (properties != null) {
            for (Entry<String, String> p : properties.entrySet()) {
                jsonRoot.put(p.getKey(), p.getValue());
            }
        }
        return jsonRoot;
    }

    /*
     * Recursive walk the dot-separated propertyName tree to get .skhandlers
     * property value.
     */
    private String getSkHandlersProperty(LogManager mgr, String propertyName) {
        String handlersProperty = null;
        while (propertyName != null) {
            handlersProperty = mgr.getProperty(propertyName + SK_HANDLERS);
            if (handlersProperty != null || propertyName.isEmpty()) {
                return handlersProperty;
            }
            final int parentIndex = propertyName.lastIndexOf('.');
            if (parentIndex < 0) {
                propertyName = ""; // set to root
            } else {
                propertyName = propertyName.substring(0, parentIndex);
            }
        }
        return handlersProperty;
    }

    private void addHandlers(String componentId, String fileName) {
        /* check if the loggerName has been set to a different FileName yet. */
        final String oldFileName = LOG_FILE_MAP.putIfAbsent(logger.getName(),
                                                            fileName);
        if (oldFileName != null && !oldFileName.equals(fileName)) {
            throw new IllegalArgumentException(
                "Don't allow the same loggerName but with different fileName");
        }

        /* Check whether the logger already has existing handlers. */
        boolean hasConsoleHandler = false;
        boolean hasFileHandler = false;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        final Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                minHandlerLevel = Math.min(h.getLevel().intValue(),
                                           minHandlerLevel);
                if (h instanceof ConsoleHandler) {
                    hasConsoleHandler = true;
                } else if (h instanceof FileHandler) {
                    hasFileHandler = true;
                }
            }
        }

        final LogManager mgr = LogManager.getLogManager();
        String handlersProperty = getSkHandlersProperty(mgr, logger.getName());
        if (handlersProperty == null) {
            return;
        }
        handlersProperty = handlersProperty.toLowerCase();
        if (!hasFileHandler && handlersProperty.contains(SK_FILEHANDLER)) {
            Handler existing = FILE_HANDLER_MAP.get(fileName);
            if (existing != null) {
                /*
                 * If we support change FileHandle property in the future, then
                 * we need verify if FileHandler level/count/limit/append
                 * property is the same.
                 */
                addHandler(existing);
            } else {
                try {
                    final String dir =
                        mgr.getProperty(SK_FILEHANDLER_DIR);
                    String pattern = "%h/sklogger%u.log";
                    if (dir != null) {
                        File parent = new File(dir);
                        parent.mkdirs();
                        pattern = new File(parent, fileName).getAbsolutePath();
                    }
                    final String countProperty =
                        mgr.getProperty(SK_FILEHANDLER_COUNT);
                    int count = 10;
                    if (countProperty != null) {
                        count = Integer.parseInt(countProperty);
                    }
                    final String limitProperty =
                        mgr.getProperty(SK_FILEHANDLER_LIMIT);
                    int limit = 2000000;
                    if (limitProperty != null) {
                        limit = Integer.parseInt(limitProperty);
                    }
                    final String appendProperty =
                        mgr.getProperty(SK_FILEHANDLER_APPEND);
                    boolean append = true;
                    if (appendProperty != null) {
                        append = Boolean.parseBoolean(appendProperty);
                    }
                    final String levelProperty =
                        mgr.getProperty(SK_FILEHANDLER_LEVEL);
                    Level level = Level.ALL;
                    if (levelProperty != null) {
                        level = Level.parse(levelProperty);
                    }
                    final FileHandler fileHandler =
                        new FileHandler(componentId, pattern, limit, count,
                                        append, level);
                    existing = FILE_HANDLER_MAP.putIfAbsent(fileName,
                                                            fileHandler);
                    if (existing == null) {
                        addHandler(fileHandler);
                    } else {
                        /*
                         * Something else beat us to the unch and registered a
                         * FileHandler, so we won't be using the one we created.
                         * Release its files.
                         */
                        fileHandler.close();
                        addHandler(existing);
                    }
                } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }
        if (!hasConsoleHandler &&
            handlersProperty.contains(SK_CONSOLEHANDLER)) {
            final String levelProperty =
                mgr.getProperty(SK_CONSOLEHANDLER_LEVEL);
            Level level = Level.ALL;
            if (levelProperty != null) {
                level = Level.parse(levelProperty);
            }
            final ConsoleHandler handler = new ConsoleHandler(componentId,
                                                              level);
            addHandler(handler);
        }
    }

    /**
     * Log the event and it will be collected by Monitor system.
     */
    public void logEvent(String category,
                         Level level,
                         String subject,
                         String message,
                         Throwable cause) {
        if (!isLoggable(level)) {
            return;
        }
        process(new StringEvent(category, level, subject, message, cause));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return logger.getName();
    }

    /**
     * @return the wrapped logger for normal tracing log.
     */
    public Logger getLogger() {
        return logger;
    }

    // java.util.logging.Logger decorator

    /**
     * If it is expensive to construct logging message, do logging and handler
     * level pre-check.
     */
    public boolean isLoggable(Level level) {
        if (!logger.isLoggable(level) ||
            minHandlerLevel > level.intValue()) {
            return false;
        }
        return true;
    }

    /**
     * Determine whether logging is enabled for this level, in the given
     * logging context.  The context can only enable logging where it
     * would normally not be enabled, were the context not present.
     */
    public boolean isLoggable(Level level, LogContext lc) {
        if (isLoggable(level)) {
            return true;
        }
        if (lc == null) {
            /* No log context is given */
            return false;
        }
        final int contextLevelValue = lc.getLogLevel();
        if (level.intValue() < contextLevelValue) {
            return false;
        }
        /* Allow the context's log level override the Logger's. */
        return true;
    }

    /**
     * @see Logger#addHandler(Handler)
     */
    public void addHandler(Handler handler) throws SecurityException {
        minHandlerLevel = Math.min(handler.getLevel().intValue(),
                                   minHandlerLevel);
        logger.addHandler(handler);
    }

    /**
     * @see Logger#severe(String)
     */
    public void severe(String msg) {
        log(Level.SEVERE, msg);
    }

    public void severe(String msg, LogContext lc) {
        log(Level.SEVERE, msg, lc);
    }

    /**
     * @see Logger#warning(String)
     */
    public void warning(String msg) {
        log(Level.WARNING, msg);
    }

    public void warning(String msg, LogContext lc) {
        log(Level.WARNING, msg, lc);
    }

    /**
     * @see Logger#info(String)
     */
    public void info(String msg) {
        log(Level.INFO, msg);
    }

    public void info(String msg, LogContext lc) {
        log(Level.INFO, msg, lc);
    }

    /**
     * @see Logger#fine(String)
     */
    public void fine(String msg) {
        log(Level.FINE, msg);
    }

    public void fine(String msg, LogContext lc) {
        log(Level.FINE, msg, lc);
    }

    /**
     * @see Logger#log(Level, String)
     */
    public void log(Level level, String msg) {
        logger.log(level, msg);
    }

    /**
     * Log message with context.
     */
    public void log(Level level, String msg, LogContext ctx) {
        /* Can't use try-with-resources in java 6
        try (WithLogContext wlc = new WithLogContext(ctx)) {
            log(level, msg);
        }
        */
        WithLogContext wlc = new WithLogContext(ctx);
        try {
            log(level, msg);
        } finally {
            wlc.close();
        }
    }

    /**
     * @see Logger#log(Level, String, Throwable)
     */
    public void log(Level level, String msg, Throwable thrown) {
        logger.log(level, msg, thrown);
    }

    /**
     * @see Logger#setLevel(Level)
     */
    public void setLevel(Level newLevel) throws SecurityException {
        logger.setLevel(newLevel);
    }

    /**
     * @see Logger#setUseParentHandlers(boolean)
     */
    public void setUseParentHandlers(boolean useParentHandlers) {
        logger.setUseParentHandlers(useParentHandlers);
    }

    /**
     * @see Logger#getLevel()
     */
    public Level getLevel() {
        return logger.getLevel();
    }

    /**
     * @see Logger#getParent()
     */
    public Logger getParent() {
        return logger.getParent();
    }

    /**
     * Additional logging level defined for monitor logging.
     *<pre>
     * Note that the new order of logging level after introduced monitor
     * logging level is as below:
     * - SEVERE
     * - METRIC
     * - WARNING
     *</pre>
     * Also there is a new logging level EVENT that is dynamic level value.
     * Monitor logging only collects a limited set of logging information of
     * monitor-relevant activities.
     */
    private static final class MonitorLevel extends Level {
        private static final long serialVersionUID = 1L;

        private static final int METRIC_VALUE = 902;

        // EVENT value is original level value plus one.
        private static final int EVENT_SEVERE_VALUE = SEVERE.intValue() + 1;
        private static final int EVENT_WARNING_VALUE = WARNING.intValue() + 1;
        private static final int EVENT_INFO_VALUE = INFO.intValue() + 1;
        private static final int EVENT_CONFIG_VALUE = CONFIG.intValue() + 1;
        private static final int EVENT_FINE_VALUE = FINE.intValue() + 1;
        private static final int EVENT_FINER_VALUE = FINER.intValue() + 1;
        private static final int EVENT_FINEST_VALUE = FINEST.intValue() + 1;

        public static final MonitorLevel METRIC =
            new MonitorLevel("METRIC", METRIC_VALUE);

        /*
         * Cache for common event, to avoid repeating new MonitorLevel
         */
        public static final MonitorLevel EVENT_SEVERE =
            new MonitorLevel("EVENT", EVENT_SEVERE_VALUE);

        public static final MonitorLevel EVENT_WARNING =
            new MonitorLevel("EVENT", EVENT_WARNING_VALUE);

        public static final MonitorLevel EVENT_INFO =
            new MonitorLevel("EVENT", EVENT_INFO_VALUE);

        public static final MonitorLevel EVENT_CONFIG =
            new MonitorLevel("EVENT", EVENT_CONFIG_VALUE);

        public static final MonitorLevel EVENT_FINE =
            new MonitorLevel("EVENT", EVENT_FINE_VALUE);

        public static final MonitorLevel EVENT_FINER =
            new MonitorLevel("EVENT", EVENT_FINER_VALUE);

        public static final MonitorLevel EVENT_FINEST =
            new MonitorLevel("EVENT", EVENT_FINEST_VALUE);

        private MonitorLevel(String name, int value) {
            super(name, value);
        }

        public static MonitorLevel getEventLevel(int value) {
            ++value; // increase one to change to EVENT value.
            if (value == EVENT_SEVERE_VALUE) {
                return EVENT_SEVERE;
            }
            if (value == EVENT_WARNING_VALUE) {
                return EVENT_WARNING;
            }
            if (value == EVENT_INFO_VALUE) {
                return EVENT_INFO;
            }
            if (value == EVENT_CONFIG_VALUE) {
                return EVENT_CONFIG;
            }
            if (value == EVENT_FINE_VALUE) {
                return EVENT_FINE;
            }
            if (value == EVENT_FINER_VALUE) {
                return EVENT_FINER;
            }
            if (value == EVENT_FINEST_VALUE) {
                return EVENT_FINEST;
            }
            // new MonitorLevel for unknown level value.
            return new MonitorLevel("EVENT", value);
        }

        /**
         * Relies on the unique value defined for monitor logging level
         * to resolve and return designated object.
         */
        private Object readResolve() {
            final int value = intValue();
            if (value == METRIC_VALUE) {
                return METRIC;
            }
            if (value == EVENT_SEVERE_VALUE) {
                return EVENT_SEVERE;
            }
            if (value == EVENT_WARNING_VALUE) {
                return EVENT_WARNING;
            }
            if (value == EVENT_INFO_VALUE) {
                return EVENT_INFO;
            }
            if (value == EVENT_CONFIG_VALUE) {
                return EVENT_CONFIG;
            }
            if (value == EVENT_FINE_VALUE) {
                return EVENT_FINE;
            }
            if (value == EVENT_FINER_VALUE) {
                return EVENT_FINER;
            }
            if (value == EVENT_FINEST_VALUE) {
                return EVENT_FINEST;
            }
            // new EVENT MonitorLevel for unknown level value.
            return new MonitorLevel("EVENT", value);
        }
    }

    /**
     * Handler is set to use SkLogger's formatter.
     * We add one more FileHandler so that we can specify SkLogger to a
     * separated FileHandler. For example Netty might use standard
     * java.util.logging.FileHandler and SkLogger use
     * oracle.kv.impl.util.sklogger.SkLogger.FileHandler
     */
    public static class FileHandler extends java.util.logging.FileHandler {

        public FileHandler(String componentId,
                           String pattern,
                           int limit,
                           int count,
                           boolean append,
                           Level level)
            throws IOException, SecurityException {
            super(pattern, limit, count, append);
            setFormatter(new ContextFormatter(componentId));
            setLevel(level);
        }
    }

    /**
     * Handler is set to use SkLogger's formatter.
     * We add one more ConsoleHandler so that we can specify SkLogger to a
     * separated ConsoleHandler. For example Netty might use standard
     * java.util.logging.ConsoleHandler and SkLogger use
     * oracle.kv.impl.util.sklogger.SkLogger.ConsoleHandler
     */
    public static class ConsoleHandler
        extends java.util.logging.ConsoleHandler {

        public ConsoleHandler(String componentId, Level level) {
            super();
            setFormatter(new ContextFormatter(componentId));
            setLevel(level);
        }
    }
}
