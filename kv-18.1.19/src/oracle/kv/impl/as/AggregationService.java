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

package oracle.kv.impl.as;

import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.KVVersion.CURRENT_VERSION;

import java.io.IOException;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/*import java.util.concurrent.atomic.AtomicLong;*/
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KVStoreFactory;
import oracle.kv.StatementResult;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.ResourceInfo;
import oracle.kv.impl.rep.admin.ResourceInfo.RateRecord;
import oracle.kv.impl.rep.admin.ResourceInfo.UsageRecord;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.JsonUtils;
import oracle.kv.impl.util.TopologyLocator;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;
import oracle.kv.util.Ping.ExitCode;
import oracle.kv.util.shell.ShellCommandResult;

import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ObjectNode;

public class AggregationService {
    // TODO - Move back when deprecated constructor and method are removed
    private static final int PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC = 60;
    private static final int PEAK_THROUGHPUT_DEFAULT_TTL_DAY = 14;

    /*
     * Peak throughput history table definition.
     */
    public static final String PEAK_TABLE_NAME = "PeakThroughput";
    public static final int PEAK_TABLE_VERSION = 1;
    public static final String PEAK_TABLE_ID_FIELD_NAME = "id";
    public static final String PEAK_START_SECOND_FIELD_NAME = "startSecond";
    public static final String PEAK_READ_KB_FIELD_NAME = "peakReadKB";
    public static final String PEAK_WRITE_KB_FIELD_NAME = "peakWriteKB";

    private static final Logger logger =
                        Logger.getLogger(AggregationService.class.getName());

    private final KVStore kvStore;
    private final TableAPI tableAPI;

    private final TableSizeAggregator tableAggregator;
    private final ScheduledExecutorService executor;
    private final int throughputPollPeriodSec;
    private final int tableSizePollPeriodSec;
    private final int peakThroughputCollectionPeriodSec;
    private final TimeToLive peakThroughputTTL;

    private LoginManager loginManager;

    private volatile boolean stop = false;
    private volatile boolean started = false;
    private volatile Topology topology;

    private long lastCallMillis;

    private volatile Collection<UsageRecord> sizeUsageRecords = null;

    /*
     * Map of table ID to peak throughput records. The map is replaced during
     * at the start of each collection period.
     */
    private volatile Map<Long, PeakRecord> peakRecords;

    /*
     * Starting second for the peak data set. This is initialized to
     * MAX_VALUE and set to the earliest time in found.
     */
    /* TODO - Because of the restriction to Java6 RateRecord cannot implement
     * LongUnaryOperator which would allow the use of AtomicLong for
     * peakStartSecond. So we need to synchronize setting and access.
     */
  /*private final AtomicLong peakStartSecond = new AtomicLong(Long.MAX_VALUE);*/
    private long peakStartSecond = Long.MAX_VALUE;

    /* Cached handle to the peak throughout table */
    private Table peakTable = null;

    /*
     * set by a caller if an instance of AS is created in-process.
     * it is used to allow clean stop/shutdown.
     */
    private Thread aggThread;

    @Deprecated
    public AggregationService(String storeName,
                              List<String> hostPorts,
                              int throughputPollPeriodSec,
                              int tableSizePollPeriodSec,
                              int maxThreads)
            throws KVStoreException {
        this(storeName, hostPorts,
             throughputPollPeriodSec, tableSizePollPeriodSec,
             PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC,
             PEAK_THROUGHPUT_DEFAULT_TTL_DAY,
             maxThreads);
    }

    public AggregationService(String storeName,
                              List<String> hostPorts,
                              int throughputPollPeriodSec,
                              int tableSizePollPeriodSec,
                              int peakThroughputCollectionPeriodSec,
                              int peakThroughputTTLDay,
                              int maxThreads)
            throws KVStoreException {
        if (throughputPollPeriodSec < 1) {
            throw new IllegalArgumentException("Throughput poll period" +
                                               " must be > 0");
        }
        if (tableSizePollPeriodSec < 1) {
            throw new IllegalArgumentException("Table size poll period" +
                                               " must be > 0");
        }
        if (peakThroughputCollectionPeriodSec < 1) {
            throw new IllegalArgumentException("Peak throughput collection" +
                                               " period must be > 0");
        }
        if (peakThroughputTTLDay < 1) {
            throw new IllegalArgumentException("Peak throughput TTL" +
                                               " must be > 0");
        }
        this.throughputPollPeriodSec = throughputPollPeriodSec;
        this.tableSizePollPeriodSec = tableSizePollPeriodSec;
        this.peakThroughputCollectionPeriodSec =
                                        peakThroughputCollectionPeriodSec;
        peakThroughputTTL = TimeToLive.ofDays(peakThroughputTTLDay);
        peakRecords = new ConcurrentHashMap<>();

        logger.log(Level.INFO,
                   "Starting AggregationService {0} for {1}," +
                   " throughput poll period: {2} seconds," +
                   " table size poll period: {3} seconds," +
                   " peak throughput collection period: {4} seconds," +
                   " peak throughput TTL: {5} days",
                   new Object[]{CURRENT_VERSION.getNumericVersionString(),
                                storeName, throughputPollPeriodSec,
                                tableSizePollPeriodSec,
                                peakThroughputCollectionPeriodSec,
                                peakThroughputTTLDay});
        final KVStoreConfig kvConfig = new KVStoreConfig(storeName,
                                                         hostPorts.get(0));
        kvStore = KVStoreFactory.getStore(kvConfig);
        tableAPI = kvStore.getTableAPI();
        loginManager = KVStoreImpl.getLoginManager(kvStore);

        topology = findTopo(hostPorts, maxThreads);
        assert topology != null;
        logger.log(Level.INFO, "Initial topology seq# {0}",
                   topology.getSequenceNumber());

        tableAggregator = new TableSizeAggregator(tableAPI, logger);
        executor = Executors.newScheduledThreadPool(maxThreads);
    }

    /* Synchronously execute the polling loop. */
    public void startPolling() throws InterruptedException {
        if (started) {
            return;
        }
        started = true;
        start();
    }

    private void start()
        throws InterruptedException {

        try {
            /*
             * Schedule a task to collect size information. At each call to
             * getTablesSizes sizeUsageRecords with be set to the latest size
             * information. The usage records, if any, are sent to the
             * RNs when polling for throughput information.
             */
            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        sizeUsageRecords = tableAggregator.getTableSizes();
                    } catch (Exception e) {
                        logger.log(Level.SEVERE,
                                  "Unexpected exception collecting table sizes",
                                   e);
                        stop = true;
                    }
                }
            }, 0L, /* initialDelay */
            tableSizePollPeriodSec, SECONDS);

            /*
             * Schedule a task to export peak throughput information.
             */
            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        exportPeakThroughput();
                    } catch (Exception e) {
                        /*
                         * Note that we do not treat failures exporting peak
                         * throughout as fatal. We log the failure and export
                         * will be retried at the end of the next collection
                         * period. If the failures continue, hopefully the
                         * health monitor will take action.
                         */
                        logger.log(Level.WARNING,
                                   "Unexpected exception exporting peak" +
                                   " throughput", e);
                    }
                }
            },
            /*
             * Delay the initial run so that there is time to collect
             * peak data.
             */
            peakThroughputCollectionPeriodSec,  /* initialDelay */
            peakThroughputCollectionPeriodSec,  /* period */
            SECONDS);

            /*
             * Initialize the last call to be a poll period ago. This should
             * get the usual history to start things off.
             */
            final long periodMillis = SECONDS.toMillis(throughputPollPeriodSec);
            lastCallMillis = System.currentTimeMillis() - periodMillis;

            /*
             * Throughput polling loop. Note that table size limits are sent
             * to the RNs here, not in the thread collecting size info.
             */
            while (!stop) {
                final long startMillis = System.currentTimeMillis();
                pollThroughput();
                lastCallMillis = startMillis;

                /*
                 * If the poll took less than the period, sleep for remaining
                 * time.
                 */
                final long finishMillis = System.currentTimeMillis();
                // TODO - need to turn duration into something that can be
                // used to monitor the health of the AS and/or the store.
                final long durationMillis = finishMillis - startMillis;
                final long remaining = periodMillis - durationMillis;
                if (!stop && (remaining > 0)) {
                    Thread.sleep(remaining);
                }
            }
        } finally {
            executor.shutdownNow();
        }
        logger.info("Shutdown");
    }

    /**
     * Polls the RNs for throughput information. During the poll the size
     * records, if any, are sent to each node. Once the throughput information
     * is collected, the rates are aggregated and if any limits are exceeded
     * the limit records are sent to all of the RNs.
     */
    private void pollThroughput() throws InterruptedException {

        /*
         * Create a local reference to tableSizeRecords since it may change.
         * The size limit records only need to be sent once. So if there
         * are no errors calling the RNs, the records can be cleared. It will
         * be OK to modify the collection using this reference (see below)
         * since the contents are not changed elsewhere.
         */
        final Collection<UsageRecord> sizeRecords = sizeUsageRecords;

        /*
         * Call all nodes to get resource info since lastCallMills. Send
         * any table size limit records.
         */
        final List<Future<ResourceInfo>> results =
                                    callAllNodes(lastCallMillis, sizeRecords);
        if (results == null) {
            return;
        }

        final TreeSet<RateRecord> records = new TreeSet<>();

        /*  Keep track of errors so we can clear the size limit records. */
        int errors = 0;

        /*
         * Collect all of the returned rate records. They are sorted by second.
         */
        for (Future<ResourceInfo> f : results) {
            if (!f.isDone() || f.isCancelled()) {
                logger.log(Level.FINE,
                           "Task did not complete or was canceled: {0}",
                           f.isCancelled());
                errors++;
            }
            try {
                final ResourceInfo info = f.get();
                if (info == null) {
                    errors++;
                    continue;
                }
                records.addAll(info.getRateRecords());
            } catch (ExecutionException ex) {
                errors++;
                logger.log(Level.INFO, "Task failed {0}", ex);
            }
        }
        logger.log(Level.FINE, "Collected {0} records, {1} errors",
                   new Object[]{records.size(), errors});

        /*
         * Iterate through the rate records in second order. For each
         * second accumulate the rates in the accumulators map. Then
         * see if any were over for that second. Any overages will generate
         * a record in the overageRecords map.
         */
        final Map<Long, ThroughputAccumulator> accumulators = new HashMap<>();
        final Map<Long, UsageRecord> throughputUsageMap = new HashMap<>();

        long second = 0L;
        for (RateRecord rr : records) {
            /* Set second to earliest seen. Uses LongUnaryOperator to update */
            /*peakStartSecond.getAndUpdate(rr);*/
            updatePeakStartSecond(rr.getSecond());

            if (second != rr.getSecond()) {
                /* new second, see if any of the accumulated records are over */
                getUsageRecords(accumulators, throughputUsageMap);
                second = rr.getSecond();
                updatePeakRecords(accumulators);
                accumulators.clear();
            }
            final long tableId = rr.getTableId();
            final ThroughputAccumulator accumulator = accumulators.get(tableId);
            if (accumulator == null) {
                accumulators.put(tableId, new ThroughputAccumulator(rr));
            } else {
                accumulator.add(rr);
            }
        }
        updatePeakRecords(accumulators);
        getUsageRecords(accumulators, throughputUsageMap);

        /* If there were throughput overages, send them. */
        if (!throughputUsageMap.isEmpty()) {
            callAllNodes(0, new ArrayList<>(throughputUsageMap.values()));
        }

        /*
         * If there were no errors, we can safely remove size limit records.
         */
        if ((errors == 0) && (sizeRecords != null)) {
            sizeRecords.clear();
        }
    }

    private synchronized void updatePeakStartSecond(long second) {
        if (second < peakStartSecond) {
            peakStartSecond = second;
        }
    }

    private synchronized long getAndResetPeakStartSecond() {
        final long ret = peakStartSecond;
        peakStartSecond = Long.MAX_VALUE;
        return ret;
    }

    /**
     * Creates usage records from the specified accumulators.
     */
    private void getUsageRecords(Map<Long, ThroughputAccumulator> accumulators,
                                 Map<Long, UsageRecord> throughputUsageMap) {
        for (ThroughputAccumulator ta : accumulators.values()) {
            if (ta.isOver()) {
                /*
                 * There may already be a record for this table. If so just
                 * skip as there is no need for more than one.
                 */
                if (!throughputUsageMap.containsKey(ta.tableId)) {
                    throughputUsageMap.put(ta.tableId,
                                       new UsageRecord(ta.tableId,
                                                       ta.readKB, ta.writeKB));
                }
            }
        }
    }

    /**
     * Updates the peak record from the specified throughput accumulators.
     * Peak records are created and added to the peakRecords map as needed.
     */
    private void
              updatePeakRecords(Map<Long, ThroughputAccumulator> accumulators) {
        for (ThroughputAccumulator ta : accumulators.values()) {
             /*
             * Track peak throughput which is independent of the second. Note
             * that peakRecords may be refreshed between the get and set. That
             * is OK because pr will also be missing from the new (empty) map.
             */
            final PeakRecord pr = peakRecords.get(ta.tableId);
            if (pr == null) {
                peakRecords.put(ta.tableId,
                                new PeakRecord(ta.readKB, ta.writeKB));
            } else {
                pr.update(ta.readKB, ta.writeKB);
            }
        }
    }

    /**
     * Calls getResourceInfo on all RNs in the store, returning the list of
     * futures with the results. The lastCall and usageRecords are passed to
     * the getResourceInfo method. If usageRecords is empty, null is sent.
     */
    private List<Future<ResourceInfo>>
                    callAllNodes(long lastCall,
                                 Collection<UsageRecord> usageRecords)
                            throws InterruptedException {
        /* Send null if there are no records */
        final Collection<UsageRecord> usageRecord =
              ((usageRecords != null) && usageRecords.isEmpty()) ? null :
                                                                   usageRecords;

        final List<Callable<ResourceInfo>> tasks = new ArrayList<>();
        for (RepGroup rg : topology.getRepGroupMap().getAll()) {
            /* Generate tasks for each group */
            final RepGroup group = topology.get(rg.getResourceId());
            if (group == null) {
                logger.log(Level.INFO, "{0} missing from topo seq# {1}",
                           new Object[]{rg, topology.getSequenceNumber()});
                continue;
            }

            /* LoginManager not needed ??? */
            final RegistryUtils regUtils = new RegistryUtils(topology,
                                                             loginManager);
            for (final RepNode rn : group.getRepNodes()) {
                tasks.add(new Callable<ResourceInfo>() {
                    @Override
                    public ResourceInfo call() throws Exception {
                        try {
                            final RepNodeAdminAPI rna =
                                  regUtils.getRepNodeAdmin(rn.getResourceId());

                            // TODO - do something with this?
                            //rna.getInfo().getSoftwareVersion();
                            final ResourceInfo info =
                                rna.exchangeResourceInfo(lastCall, usageRecord);
                            // TODO - info can be null????
                            checkTopology(info, rna);
                            return info;
                        } catch (RemoteException | NotBoundException re) {
                            logger.log(Level.WARNING,
                                       "Unexpected exception calling {0}: {1}",
                                       new Object[]{rn.getResourceId(), re});
                        }
                        /* Returning null will be recorded as an error */
                        return null;
                    }
                });
            }
        }
        return tasks.isEmpty() ? Collections.emptyList() :
                                 executor.invokeAll(tasks,
                                                    throughputPollPeriodSec,
                                                    TimeUnit.SECONDS);
    }

    /**
     * Writes a row to the peak throughput table for each non-empty
     * PeakRecord. The peakRecords map is recreated, and the peakSecond is
     * reset.
     */
    private void exportPeakThroughput() throws Exception {
        //final int startSecond = (int)peakStartSecond.getAndSet(Long.MAX_VALUE);
        final int startSecond = (int)getAndResetPeakStartSecond();
        final Map<Long, PeakRecord> prMap = peakRecords;
        peakRecords = new ConcurrentHashMap<>();

        final Table table = getPeakTable();
        assert table != null;

        for (Entry<Long, PeakRecord> e : prMap.entrySet()) {
            final PeakRecord pr = e.getValue();

            if (pr.hasPeak()) {
                final long tableId = e.getKey();
                logger.log(Level.FINE, "Peak for {0} starting at {1} {2}",
                           new Object[]{tableId, startSecond, pr});
                final Row row = table.createRow();
                row.put(PEAK_TABLE_ID_FIELD_NAME, tableId);
                row.put(PEAK_START_SECOND_FIELD_NAME, startSecond);
                row.put(PEAK_READ_KB_FIELD_NAME, pr.peakReadKB);
                row.put(PEAK_WRITE_KB_FIELD_NAME, pr.peakWriteKB);
                /* Set the TTL in case it is different from the table defult */
                row.setTTL(peakThroughputTTL);
                tableAPI.put(row, null, null);
            }
        }
    }

    /**
     * Gets the peak throughout table handle. The table is created if it does
     * not exist. The table handle is cached.
     */
    private Table getPeakTable() throws Exception {
        if (peakTable != null) {
            return peakTable;
        }

        peakTable = tableAPI.getTable(PEAK_TABLE_NAME);
        if (peakTable != null) {
            final int tableVersion =
                        Integer.parseInt(peakTable.getDescription());
            logger.log(Level.FINE, "Found " + PEAK_TABLE_NAME +
                       " version {0}", tableVersion);
            if (tableVersion > PEAK_TABLE_VERSION) {
                throw new Exception(PEAK_TABLE_NAME + " is at version " +
                                    tableVersion + " please upgrade the " +
                                    "aggregration service");
            }
            /*
             * TODO - Currently changing the default TTL on a table does not
             * affect existing records. If this changes, it would be worth
             * checking if the input TTL is different than the table's default
             * and if so change the table default.
             */
            return peakTable;
        }

        logger.info("Creating peak table");
        final String createDML =
                "CREATE TABLE " + PEAK_TABLE_NAME + " " +
                "COMMENT \"" + PEAK_TABLE_VERSION + "\" (" +
                        PEAK_TABLE_ID_FIELD_NAME + " LONG, " +
                        PEAK_START_SECOND_FIELD_NAME + " INTEGER, " +
                        PEAK_READ_KB_FIELD_NAME + " INTEGER, " +
                        PEAK_WRITE_KB_FIELD_NAME + " INTEGER, " +
                        "PRIMARY KEY(" + PEAK_TABLE_ID_FIELD_NAME + ", " +
                                         PEAK_START_SECOND_FIELD_NAME + ")) " +
                "USING TTL " + peakThroughputTTL.getValue() + " DAYS";

        final StatementResult result = kvStore.executeSync(createDML);
        if (!result.isSuccessful()) {
            throw new Exception("Failed to create " +
                                PEAK_TABLE_NAME + ": " + result);
        }

        peakTable = tableAPI.getTable(PEAK_TABLE_NAME);
        if (peakTable == null) {
            throw new Exception("Unable to get " + PEAK_TABLE_NAME);
        }
        return peakTable;
    }

    /*
     * Object to record per-table peak throughput information.
     */
    private static class PeakRecord {
        private int peakReadKB;
        private int peakWriteKB;

        private PeakRecord(int readKB, int writeKB) {
            peakReadKB = readKB;
            peakWriteKB = writeKB;
        }

        /*
         * Updates the peak read and write peak data if the input values are
         * greater.
         */
        private void update(int readKB, int writeKB) {
            if (readKB > peakReadKB) {
                peakReadKB = readKB;
            }
            if (writeKB > peakWriteKB) {
                peakWriteKB = writeKB;
            }
        }

        /*
         * Returns true if the record has non-zero peak read or write
         * throughput data.
         */
        private boolean hasPeak() {
            return peakReadKB > 0 || peakWriteKB > 0;
        }

        @Override
        public String toString() {
            return "PeakRecord[" + peakReadKB + ", " + peakWriteKB + "]";
        }
    }

    private static class ThroughputAccumulator {
        private final long tableId;
        private final int readLimitKB;
        private final int writeLimitKB;

        private int readKB;
        private int writeKB;

        ThroughputAccumulator(RateRecord rr) {
            tableId = rr.getTableId();
            readLimitKB = rr.getReadLimitKB();
            writeLimitKB = rr.getWriteLimitKB();
            add(rr);
        }

        private void add(RateRecord rr) {
            assert rr.getTableId() == tableId;
            readKB += rr.getReadKB();
            writeKB += rr.getWriteKB();
        }

        public boolean isOver() {
            return readKB > readLimitKB ||
                   writeKB > writeLimitKB;
        }

        @Override
        public String toString() {
            return "ThroughputAccumulator[" + tableId + ", " +
                   readKB + ", " + readLimitKB + ", " +
                   writeKB + ", " + writeLimitKB + "]";
        }
    }

    private Topology findTopo(List<String> hostPorts, int maxThreads)
        throws KVStoreException {

        if (hostPorts == null) {
            throw new IllegalArgumentException("null hosts ports");
        }

        String[] hostPortsArray = new String[hostPorts.size()];
        hostPortsArray = hostPorts.toArray(hostPortsArray);

        /* Search available SNs for a topology */
        Topology newtopo = null;

        /*
         * The search for a new topo is confined to SNs that host RNs. If
         * Admins live on SNs which don't host RNs, we'll be delayed in
         * seeing a new topo; we'd have to wait for that to be propagated to
         * the RNs. That's ok; by design, the system will propagate topos to
         * RNs in a timely fashion, and it's not worth adding complications
         * for the unusual case of an Admin-only SN.
         */
        try {
            newtopo = TopologyLocator.get(hostPortsArray, 0,
                                          loginManager, null);
        } catch (KVStoreException topoLocEx) {
            /* had a problem getting a topology - try using the Admins */
            newtopo = searchAdminsForTopo(hostPortsArray, maxThreads);

            /* Still can't find a topology */
            if (newtopo == null) {
                throw topoLocEx;
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception locating topology: {0}", e);
        }
        return newtopo;
    }

    /**
     * Given a set of SNs, find an AdminService to find a topology
     */
    private Topology searchAdminsForTopo(String[] hostPortStrings,
                                         int maxThreads) {
        final HostPort[] targetHPs = HostPort.parse(hostPortStrings);

        /* Look for admins to get topology */
        final Collection<Callable<Topology>> tasks = new ArrayList<>();
        for (final HostPort hp : targetHPs) {
            tasks.add(new Callable<Topology>() {
                @Override
                public Topology call() throws Exception {
                    try {
                        final CommandServiceAPI admin =
                            getAdmin(hp.hostname(), hp.port());
                        return admin.getTopology();
                    } catch (RemoteException re) {
                        logger.log(Level.SEVERE,
                                   "Exception attempting to contact Admin {0}",
                                   re);
                        /*
                         * Throw out all Exceptions to tell this task failed to
                         * get topology.
                         */
                        throw re;
                    }
                }
            });
        }

        final ExecutorService es =
                Executors.newFixedThreadPool(maxThreads);
        try {
            /*
             * Returns the topology result got by the first completed task.
             */
            return es.invokeAny(tasks);
        } catch (Exception e) {
            /*
             * If it throws Exception, that means all task failed.
             * Can't find any Admins, there should be some in the list.
             */
            logger.severe("Searching for topology, can't contact any " +
                          "Admin services in the store");
            return null;
        } finally {
            es.shutdownNow();
        }
    }

    /**
     * Get the CommandService on this particular SN.
     */
    private CommandServiceAPI getAdmin(String snHostname, int snRegistryPort)
        throws NotBoundException, RemoteException {
        /*
         * Use login manager first, if it is available.
         */
        if (loginManager != null) {
            return RegistryUtils.getAdmin(snHostname, snRegistryPort,
                                          loginManager);
        }

        /*
         * Non-secure case.
         */
        return RegistryUtils.getAdmin(snHostname, snRegistryPort, null);
    }

    /**
     * Checks to see if the topology needs to be updated. The info object
     * contains the topo sequence number at that node. Check it against
     * the topo we have. If it is newer get the topo from the RN.
     */
    private void checkTopology(ResourceInfo info, RepNodeAdminAPI rna)
        throws RemoteException {
        if (info == null) {
            return;
        }
        if (topology.getSequenceNumber() >= info.getTopoSeqNum()) {
            return;
        }
        logger.log(Level.FINE, "Need to update topo, {0} >= {1}",
                   new Object[]{topology.getSequenceNumber(),
                                info.getTopoSeqNum()});
        final Topology newTopo = rna.getTopology();
        synchronized (this) {
            if (topology.getSequenceNumber() < newTopo.getSequenceNumber()) {
                logger.log(Level.FINE, "Updating to topopogy seq# {0}",
                           newTopo.getSequenceNumber());
                topology = newTopo;
            }
        }
    }

    public static final String COMMAND_NAME = "aggregationservice";
    public static final String COMMAND_DESC =
                                    "monitors resource usage of a store";
    private static final String HELPER_HOSTS_FLAG = "-helper-hosts";
    private static final String THROUGHPUT_POLL_PERIOD_FLAG =
                                                    "-throughput-poll-period";
    private static final int THROUGHPUT_POLL_PERIOD_DEFAULT_SEC = 5;
    private static final String TABLE_SIZE_POLL_PERIOD_FLAG =
                                                    "-table-size-poll-period";
    private static final int TABLE_SIZE_POLL_PERIOD_DEFAULT_SEC = 15;
    private static final String PEAK_THROUGHPUT_COLLECTION_PERIOD_FLAG =
                                           "-peak-throughput-collection-period";
// TODO - uncomment when deprecated items are removed
//    private static final int PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC = 60;
    private static final String PEAK_THROUGHPUT_TTL_FLAG =
                                                        "-peak-throughput-ttl";
// TODO - uncomment when deprecated items are removed
//    private static final int PEAK_THROUGHPUT_DEFAULT_TTL_DAY = 14;

    private static final String MAX_THREADS_FLAG = "-max-threads";
    private static final int MAX_THREADS_DEFAULT = 10;
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " +
        CommandParser.getPortUsage() + " or\n\t" +
        HELPER_HOSTS_FLAG + " <host:port[,host:port]*>\n\t" +
        THROUGHPUT_POLL_PERIOD_FLAG + " <seconds>\n\t" +
        TABLE_SIZE_POLL_PERIOD_FLAG + " <seconds>\n\t" +
        PEAK_THROUGHPUT_COLLECTION_PERIOD_FLAG + " <seconds>\n\t" +
        PEAK_THROUGHPUT_TTL_FLAG + " <days>\n\t" +
        MAX_THREADS_FLAG + " <n>\n\t" +
        CommandParser.optional(CommandParser.JSON_FLAG);

    private static class AggregationServiceParser extends CommandParser {
        private String helperHosts = null;
        private int throughputPollPeriodSec =
                                             THROUGHPUT_POLL_PERIOD_DEFAULT_SEC;
        private int tableSizePollPeriodSec = TABLE_SIZE_POLL_PERIOD_DEFAULT_SEC;
        private int peakThroughputCollectionPeriodSec =
                                  PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC;
        private int peakThroughputTTLDay = PEAK_THROUGHPUT_DEFAULT_TTL_DAY;
        private int maxThreads = MAX_THREADS_DEFAULT;

        AggregationServiceParser(String[] args1) {
            super(args1);
        }

        @Override
        public void usage(String errorMsg) {
            /*
             * Note that you can't really test illegal arguments in a
             * threaded unit test -- the call to exit(..) when
             * dontExit is false doesn't kill the process, and the error
             * message gets lost. Still worth using dontExit so the
             * unit test process doesn't die, but unit testing of bad
             * arg handling has to happen with a process.
             */
            if (!getJson()) {
                if (errorMsg != null) {
                    System.err.println(errorMsg);
                }
                System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME +
                                   "\n\t" + COMMAND_ARGS);
            }
            exit(errorMsg, ExitCode.EXIT_USAGE, System.err,
                 getJsonVersion());
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(HELPER_HOSTS_FLAG)) {
                helperHosts = nextArg(arg);
                return true;
            }

            if (arg.equals(THROUGHPUT_POLL_PERIOD_FLAG)) {
                throughputPollPeriodSec = nextIntArg(arg);
                return true;
            }

            if (arg.equals(TABLE_SIZE_POLL_PERIOD_FLAG)) {
                tableSizePollPeriodSec = nextIntArg(arg);
                return true;
            }

            if (arg.equals(PEAK_THROUGHPUT_COLLECTION_PERIOD_FLAG)) {
                peakThroughputCollectionPeriodSec = nextIntArg(arg);
                return true;
            }

            if (arg.equals(PEAK_THROUGHPUT_TTL_FLAG)) {
                peakThroughputTTLDay = nextIntArg(arg);
                return true;
            }

            if (arg.equals(MAX_THREADS_FLAG)) {
                maxThreads = nextIntArg(arg);
                return true;
            }

            return false;
        }

        @Override
        protected void verifyArgs() {
            /* Check that one or more helper hosts are supplied */
            if (helperHosts != null &&
                (getHostname() != null || (getRegistryPort() != 0))) {
                usage("Only one of either " +  HELPER_HOSTS_FLAG + " or " +
                      HOST_FLAG + " plus " + PORT_FLAG +
                      " may be specified");
            }

            if (helperHosts == null) {
                if (getHostname() == null) {
                    missingArg(HOST_FLAG);
                }
                if (getRegistryPort() == 0) {
                    missingArg(PORT_FLAG);
                }
            } else {
                /*
                 * Helper hosts have been supplied - validate the
                 * argument.
                 */
                try {
                    validateHelperHosts(helperHosts);
                } catch (IllegalArgumentException e) {
                    usage("Illegal value for " + HELPER_HOSTS_FLAG );
                }
            }
        }

        /**
         * Validate that each helper host entry in the form
         * <string>:<number>
         */
        private void validateHelperHosts(String helperHostVal)
            throws IllegalArgumentException {

            if (helperHostVal == null) {
                throw new IllegalArgumentException
                    ("helper hosts cannot be null");
            }
            HostPort.parse(helperHostVal.split(","));
        }

        /**
         * Return a list of hostport strings. Assumes that an argument
         * to helperHosts has already been validated.
         */
        List<String> createHostPortList() {
            final String[] hosts;
            if (helperHosts != null) {
                hosts = helperHosts.split(",");
            } else {
                hosts = new String[1];
                hosts[0] = getHostname() + ":" + getRegistryPort();
            }
            final HostPort[] hps = HostPort.parse(hosts);
            final List<String> hpList = new ArrayList<>();
            for (HostPort hp : hps) {
                hpList.add(hp.toString());
            }
            return hpList;
        }
    }

    public static void main(String[] args) {
        final AggregationServiceParser asp = new AggregationServiceParser(args);
        try {
            asp.parseArgs();
        } catch (Exception e) {
            exit("Argument error: " + e.getMessage(),
                 ExitCode.EXIT_USAGE,
                 System.err, CommandParser.getJsonVersion(args));
            return;
        }

        try {
            new AggregationService(asp.getStoreName(),
                                   asp.createHostPortList(),
                                   asp.throughputPollPeriodSec,
                                   asp.tableSizePollPeriodSec,
                                   asp.peakThroughputCollectionPeriodSec,
                                   asp.peakThroughputTTLDay,
                                   asp.maxThreads).start();
        } catch (Exception e) {
            exit("Error: " + e.getMessage(),
                 ExitCode.EXIT_UNEXPECTED,
                 System.err, asp.getJsonVersion());
        }
        exit("Service exit", ExitCode.EXIT_OK, System.out,
             asp.getJsonVersion());
    }

    /*
     * The next few methods enable starting and stopping an in-process instance
     * of AggregationService. This can be used by test code to test generation
     * of throttling exceptions, for example. The in-process instance creates a
     * thread to provide context for the polling loop in start().
     *
     * The mechanism is:
     *   AggregationService as = createAggregationService(...);
     *    // do tests
     *   as.stop(); // shutdown
     */

    /**
     * Stops an in-process instance of this service. It sets the state to "stop"
     * which tells the polling loop to end and then waits for the thread to
     * exit.
     */
    public void stop() {
        stop = true;
        if (aggThread != null) {
            try {
                aggThread.join();
            } catch (InterruptedException ie) {
                /* ignore */
            }
            aggThread = null;
        }
    }

    /**
     * Used to set the thread being used for the polling loop for an in-process
     * AS.
     */
    private void setThread(Thread aggThread) {
        this.aggThread = aggThread;
    }

    @Deprecated
    public static AggregationService createAggregationService(
        String storeName,
        String[] hostPorts,
        int throughputPollPeriodSec,
        int tableSizePollPeriodSec,
        int maxThreads) throws Exception {

        return createAggregationService(storeName,
                                        hostPorts,
                                        throughputPollPeriodSec,
                                        tableSizePollPeriodSec,
                                  PEAK_THROUGHPUT_COLLECTION_PERIOD_DEFAULT_SEC,
                                        PEAK_THROUGHPUT_DEFAULT_TTL_DAY,
                                        maxThreads);
    }

    public static AggregationService createAggregationService(
        String storeName,
        String[] hostPorts,
        int throughputPollPeriodSec,
        int tableSizePollPeriodSec,
        int peakThroughputCollectionPeriodSec,
        int peakThroughputTTLDay,
        int maxThreads) throws Exception {

        final AggregationService as =
            new AggregationService(storeName,
                                   Arrays.asList(hostPorts),
                                   throughputPollPeriodSec,
                                   tableSizePollPeriodSec,
                                   peakThroughputCollectionPeriodSec,
                                   peakThroughputTTLDay,
                                   maxThreads);

        /*
         * This thread provides context for the polling loop used by start()
         */
        final Thread aggThread = new Thread() {
                @Override
                public void run() {
                    try {
                        as.start();
                    } catch (InterruptedException ie) {
                        logger.log(Level.SEVERE,
                                   "AggregationService failed to start: {0}",
                                   ie);
                    }
                }
            };

        aggThread.start();

        /* set the thread in the instance to allow clean stop */
        as.setThread(aggThread);
        return as;
    }

    /**
     * Exit the process with the appropriate exit code, generating the
     * appropriate message.
     */
    private static void exit(String msg,
                             ExitCode exitCode,
                             PrintStream ps,
                             int jsonVersion) {
        if ((msg != null) && (ps != null)) {
            if (jsonVersion == CommandParser.JSON_V2) {
                displayExitJson(msg, exitCode, ps);
            } else if (jsonVersion == CommandParser.JSON_V1) {
                displayExitJsonV1(msg, exitCode, ps);
            } else {
                ps.println(msg);
            }
        }
        System.exit(exitCode.value());
    }

    private static final String EXIT_CODE_FIELD_V1 = "exit_code";

    private static final String EXIT_CODE_FIELD = "exitCode";

    private static void displayExitJsonV1(String msg,
                                          ExitCode exitCode,
                                          PrintStream ps) {
        final ObjectNode on = JsonUtils.createObjectNode();
        on.put(CommandJsonUtils.FIELD_OPERATION, "aggregationservice");
        on.put(CommandJsonUtils.FIELD_RETURN_CODE,
               exitCode.getErrorCode().getValue());
        final String description =
                        (msg == null) ? exitCode.getDescription() :
                                        exitCode.getDescription() + " - " + msg;
        on.put(CommandJsonUtils.FIELD_DESCRIPTION, description);
        on.put(EXIT_CODE_FIELD_V1, exitCode.value());

        /* print the json node. */
        final ObjectWriter writer = JsonUtils.createWriter(true /* pretty */);
        try {
            ps.println(writer.writeValueAsString(on));
        } catch (IOException e) {
            ps.println(e);
        }
    }

    private static void displayExitJson(String msg,
                                        ExitCode exitCode,
                                        PrintStream ps) {
        final ShellCommandResult scr =
            ShellCommandResult.getDefault("aggregationservice");
        scr.setReturnCode(exitCode.getErrorCode().getValue());
        final String description =
            (msg == null) ? exitCode.getDescription() :
                            exitCode.getDescription() + " - " + msg;
        scr.setDescription(description);
        final ObjectNode on = JsonUtils.createObjectNode();
        on.put(EXIT_CODE_FIELD, exitCode.value());
        scr.setReturnValue(on);

        try {
            ps.println(scr.convertToJson());
        } catch (IOException e) {
            ps.println(e);
        }
    }
}
