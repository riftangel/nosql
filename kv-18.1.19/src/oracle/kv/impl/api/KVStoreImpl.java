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

package oracle.kv.impl.api;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.net.ssl.SSLHandshakeException;

import oracle.kv.AsyncExecutionHandle;
import oracle.kv.AuthenticationFailureException;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.BulkWriteOptions;
import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.EntryStream;
import oracle.kv.ExecutionFuture;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreException;
import oracle.kv.KerberosCredentials;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValue;
import oracle.kv.KeyValueVersion;
import oracle.kv.LoginCredentials;
import oracle.kv.MetadataNotFoundException;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationResult;
import oracle.kv.ParallelScanIterator;
import oracle.kv.ReauthenticateHandler;
import oracle.kv.RequestTimeoutException;
import oracle.kv.ResultHandler;
import oracle.kv.ReturnValueVersion;
import oracle.kv.StatementResult;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.avro.AvroCatalogImpl;
import oracle.kv.impl.api.bulk.BulkMultiGet;
import oracle.kv.impl.api.bulk.BulkPut;
import oracle.kv.impl.api.bulk.BulkPut.KVPair;
import oracle.kv.impl.api.lob.KVLargeObjectImpl;
import oracle.kv.impl.api.ops.Delete;
import oracle.kv.impl.api.ops.DeleteIfVersion;
import oracle.kv.impl.api.ops.Execute;
import oracle.kv.impl.api.ops.Execute.OperationFactoryImpl;
import oracle.kv.impl.api.ops.Execute.OperationImpl;
import oracle.kv.impl.api.ops.Get;
import oracle.kv.impl.api.ops.InternalOperation;
import oracle.kv.impl.api.ops.MultiDelete;
import oracle.kv.impl.api.ops.MultiGet;
import oracle.kv.impl.api.ops.MultiGetIterate;
import oracle.kv.impl.api.ops.MultiGetKeys;
import oracle.kv.impl.api.ops.MultiGetKeysIterate;
import oracle.kv.impl.api.ops.Put;
import oracle.kv.impl.api.ops.PutBatch;
import oracle.kv.impl.api.ops.PutIfAbsent;
import oracle.kv.impl.api.ops.PutIfPresent;
import oracle.kv.impl.api.ops.PutIfVersion;
import oracle.kv.impl.api.ops.Result;
import oracle.kv.impl.api.ops.ResultKey;
import oracle.kv.impl.api.ops.ResultKeyValueVersion;
import oracle.kv.impl.api.ops.StoreIterate;
import oracle.kv.impl.api.ops.StoreKeysIterate;
import oracle.kv.impl.api.parallelscan.ParallelScan;
import oracle.kv.impl.api.parallelscan.ParallelScanHook;
import oracle.kv.impl.api.query.DmlFuture;
import oracle.kv.impl.api.query.InternalStatement;
import oracle.kv.impl.api.query.PreparedDdlStatementImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.rgstate.RepGroupStateTable;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableLimits;
import oracle.kv.impl.client.admin.DdlFuture;
import oracle.kv.impl.client.admin.DdlStatementExecutor;
import oracle.kv.impl.query.QueryException;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.security.SessionAccessException;
import oracle.kv.impl.security.login.KerberosClientCreds;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.login.RepNodeLoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.contextlogger.LogContext;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.query.Statement;
import oracle.kv.stats.KVStats;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TimeToLive;

import com.sleepycat.je.utilint.PropUtil;

public class KVStoreImpl implements KVStore, Cloneable {

    /* TODO: Is this the correct default value? */
    private static int DEFAULT_TTL = 5;

    /* TODO: Is this the correct default value? */
    public static int DEFAULT_ITERATOR_BATCH_SIZE = 100;

    /** A request handler to use for making all requests.  */
    private final RequestDispatcher dispatcher;

    /**
     * Indicates whether the dispatcher is owned by this instance. If we own
     * the dispatcher, our LoginManager is used to gain access to Topology
     * maintenance API methods, and so login changes should be propagated to
     * the dispatcher.
     */
    private final boolean isDispatcherOwner;

    /** Default request timeout in millis, used when timeout param is zero. */
    private final int defaultRequestTimeoutMs;

    /** Socket read timeout for requests. */
    private final int readTimeoutMs;

    /** Default Consistency, used when Consistency param is null. */
    private final Consistency defaultConsistency;

    /** Default Durability, used when Durability param is null. */
    private final Durability defaultDurability;

    /** @see KVStoreConfig#getLOBTimeout(TimeUnit) */
    private final long defaultLOBTimeout;

    /** @see KVStoreConfig#getLOBSuffix() */
    private final String defaultLOBSuffix;

    /** @see KVStoreConfig#getLOBVerificationBytes() */
    private final long defaultLOBVerificationBytes;

    /** @see KVStoreConfig#getLOBChunksPerPartition() */
    private final int defaultChunksPerPartition;

    /** @see KVStoreConfig#getLOBChunkSize() */
    private final int defaultChunkSize;

    /** @see KVStoreConfig#getCheckInterval(TimeUnit) */
    private final long checkIntervalMillis;

    /** @see KVStoreConfig#getMaxCheckRetries */
    private final int maxCheckRetries;

    /** Implementation of OperationFactory. */
    private final OperationFactoryImpl operationFactory;

    /** Max partition ID.  This value is immutable for a store. */
    private final int nPartitions;

    /** Translates between byte arrays and Keys */
    private final KeySerializer keySerializer;

    /** The component implementing large object support. */
    final KVLargeObjectImpl largeObjectImpl;

    /** Debugging and unit test hook for Parallel Scan. */
    private ParallelScanHook parallelScanHook;

    /** LoginManager - may be null*/
    private volatile LoginManager loginMgr;

    /** Login/Logout locking handle */
    private final Object loginLock = new Object();

    /** Optional reauthentication handler */
    private final ReauthenticateHandler reauthHandler;

    /**
     * The KVStore handle for the associated external store if this is an
     * internal handle, otherwise null.
     */
    private KVStoreImpl external = null;

    /**
     * Holds lazily created AvroCatalog.  In addition to acting as a volatile
     * field (see getAvroCatalog), we use an AtomicReference so the catalog is
     * shared among handles when the internal copy constructor is used (see
     * KVStoreImpl(KVStoreImpl, boolean)).
     */
    @SuppressWarnings("deprecation")
    private final AtomicReference<oracle.kv.avro.AvroCatalog> avroCatalogRef;

    private final SharedThreadPool sharedThreadPool;

    /*
     * Manages the execution of ddl statements.
     */
    final private DdlStatementExecutor statementExecutor;

    /* The default logger used on the client side. */
    private final Logger logger;

    /* TableAPI instance */
    private final TableAPIImpl tableAPI;

    /* Whether the store handle is closed */
    private volatile boolean isClosed = false;

    /**
     * The KVStoreInternalFactory constructor
     */
    public KVStoreImpl(Logger logger,
                       RequestDispatcher dispatcher,
                       KVStoreConfig config,
                       LoginManager loginMgr) {

        this(logger, dispatcher, config, loginMgr,
             (ReauthenticateHandler) null,
             false /* isDispatcherOwner */);
    }

    /**
     * The KVStoreFactory constructor
     */
    public KVStoreImpl(Logger logger,
                       RequestDispatcher dispatcher,
                       KVStoreConfig config,
                       LoginManager loginMgr,
                       ReauthenticateHandler reauthHandler) {

        this(logger, dispatcher, config, loginMgr, reauthHandler,
             true /* isDispatcherOwner */);
    }

    @SuppressWarnings("deprecation")
    private KVStoreImpl(Logger logger,
                        RequestDispatcher dispatcher,
                        KVStoreConfig config,
                        LoginManager loginMgr,
                        ReauthenticateHandler reauthHandler,
                        boolean isDispatcherOwner) {
        this.logger = logger;
        this.dispatcher = dispatcher;
        this.isDispatcherOwner = isDispatcherOwner;
        this.loginMgr = loginMgr;
        this.reauthHandler = reauthHandler;
        this.defaultRequestTimeoutMs =
            (int) config.getRequestTimeout(TimeUnit.MILLISECONDS);
        this.readTimeoutMs =
            (int) config.getSocketReadTimeout(TimeUnit.MILLISECONDS);
        this.defaultConsistency = config.getConsistency();
        this.defaultDurability = config.getDurability();
        this.checkIntervalMillis =
            config.getCheckInterval(TimeUnit.MILLISECONDS);
        this.maxCheckRetries = config.getMaxCheckRetries();
        this.keySerializer = KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        this.operationFactory = new OperationFactoryImpl(keySerializer);
        this.nPartitions =
            dispatcher.getTopology().getPartitionMap().getNPartitions();

        this.defaultLOBTimeout = config.getLOBTimeout(TimeUnit.MILLISECONDS);
        this.defaultLOBSuffix = config.getLOBSuffix();
        this.defaultLOBVerificationBytes = config.getLOBVerificationBytes();
        this.defaultChunksPerPartition = config.getLOBChunksPerPartition();
        this.defaultChunkSize = config.getLOBChunkSize();
        this.largeObjectImpl = new KVLargeObjectImpl();

        this.avroCatalogRef =
            new AtomicReference<oracle.kv.avro.AvroCatalog>(null);
        this.sharedThreadPool = new SharedThreadPool(logger);
        this.tableAPI = new TableAPIImpl(this);

        /*
         * Only invoke this after all ivs have been initialized, since it
         * creates an internal handle.
         */
        largeObjectImpl.setKVSImpl(this);

        statementExecutor = new DdlStatementExecutor(this);
    }

    public KVLargeObjectImpl getLargeObjectImpl() {
        return largeObjectImpl;
    }

    /**
     * Returns the Topology object.
     */
    public Topology getTopology() {
        return dispatcher.getTopology();
    }

    /**
     * Clones a handle for internal use, to provide access to the internal
     * keyspace (//) that is used for internal metadata.  This capability is
     * not exposed in published classes -- KVStoreConfig or KVStoreFactory --
     * in order to provide an extra safeguard against use of the internal
     * keyspace by user applications.  See KeySerializer for more information.
     * <p>
     * The new instance created by this method should never be explicitly
     * closed.  It will be discarded when the KVStoreImpl it is created from is
     * closed and discarded.
     * <p>
     * The new instance created by this method shares the KVStoreConfig
     * settings with the KVStoreImpl it is created from.  If specific values
     * are desired for consistency, durability or timeouts, these parameters
     * should be passed explicitly to the operation methods.
     */
    public static KVStore makeInternalHandle(KVStore other) {
        return new KVStoreImpl((KVStoreImpl) other,
                               true /*allowInternalKeyspace*/) {
            @Override
            public void close() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void logout() {
                throw new UnsupportedOperationException();
            }
            @Override
            public void login(LoginCredentials creds) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns the current LoginManager for a KVStore instance.
     */
    public static LoginManager getLoginManager(KVStore store) {
        final KVStoreImpl impl = (KVStoreImpl) store;
        synchronized (impl.loginLock) {
            return impl.loginMgr;
        }
    }

    /**
     * A predicate to determine whether this is an internal handle KVS handle.
     */
    private boolean isInternalHandle() {
        return keySerializer == KeySerializer.ALLOW_INTERNAL_KEYSPACE;
    }

    /**
     * Renew login manager. Only used for internal handle.
     */
    public void renewLoginManager(LoginManager loginManager) {
        if (isInternalHandle()) {
            synchronized (loginLock) {
                this.loginMgr = loginManager;
            }
        }
    }

    /**
     * Note that this copy constructor could be modified to allow overriding
     * KVStoreConfig settings, if multiple handles with difference settings are
     * needed in the future.
     */
    private KVStoreImpl(KVStoreImpl other, boolean allowInternalKeyspace) {
        this.logger = other.logger;
        this.loginMgr = getLoginManager(other);
        this.dispatcher = other.dispatcher;
        this.isDispatcherOwner = false;
        this.defaultRequestTimeoutMs = other.defaultRequestTimeoutMs;
        this.readTimeoutMs = other.readTimeoutMs;
        this.defaultConsistency = other.defaultConsistency;
        this.defaultDurability = other.defaultDurability;
        this.maxCheckRetries = other.maxCheckRetries;
        this.checkIntervalMillis = other.checkIntervalMillis;
        this.keySerializer = allowInternalKeyspace ?
            KeySerializer.ALLOW_INTERNAL_KEYSPACE :
            KeySerializer.PROHIBIT_INTERNAL_KEYSPACE;
        this.operationFactory = new OperationFactoryImpl(keySerializer);
        this.nPartitions = other.nPartitions;

        this.defaultLOBTimeout = other.defaultLOBTimeout;
        this.defaultLOBSuffix = other.defaultLOBSuffix;
        this.defaultLOBVerificationBytes = other.defaultLOBVerificationBytes;
        this.defaultChunksPerPartition = other.defaultChunksPerPartition;
        this.defaultChunkSize = other.defaultChunkSize;
        this.largeObjectImpl = other.largeObjectImpl;
        this.reauthHandler = other.reauthHandler;

        if (largeObjectImpl == null) {
            throw new IllegalStateException("null large object impl");
        }

        this.avroCatalogRef = other.avroCatalogRef;
        this.sharedThreadPool = new SharedThreadPool(logger);
        if (isInternalHandle()) {
            this.external = other;
        }

        statementExecutor = new DdlStatementExecutor(this);

        this.tableAPI = other.tableAPI;
    }

    public Logger getLogger() {
        return logger;
    }

    public KeySerializer getKeySerializer() {
        return keySerializer;
    }

    public int getNPartitions() {
        return nPartitions;
    }

    public RequestDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setParallelScanHook(ParallelScanHook parallelScanHook) {
        this.parallelScanHook = parallelScanHook;
    }

    public ParallelScanHook getParallelScanHook() {
        return parallelScanHook;
    }

    public int getDefaultRequestTimeoutMs() {
        return defaultRequestTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public long getCheckIntervalMillis() {
        return checkIntervalMillis;
    }

    public int getMaxCheckRetries() {
        return maxCheckRetries;
    }

    @Override
    public ValueVersion get(Key key)
        throws FaultException {

        return get(key, null, 0, null);
    }

    @Override
    public ValueVersion get(Key key,
                            Consistency consistency,
                            long timeout,
                            TimeUnit timeoutUnit)
        throws FaultException {

        return getInternal(key, 0, consistency, timeout, timeoutUnit, null);
    }

    public ValueVersion getInternal(Key key,
                                    long tableId,
                                    Consistency consistency,
                                    long timeout,
                                    TimeUnit timeoutUnit,
                                    LogContext lc)
        throws FaultException {

        final Request req = makeGetRequest(key, tableId, consistency, timeout,
                                           timeoutUnit, lc);
        final Result result = executeRequest(req);
        return processGetResult(result);
    }

    public static ValueVersion processGetResult(Result result) {
        final Value value = result.getPreviousValue();
        if (value == null) {
            assert !result.getSuccess();
            return null;
        }
        assert result.getSuccess();
        final ValueVersion ret = new ValueVersion();
        ret.setValue(value);
        ret.setVersion(result.getPreviousVersion());
        return ret;
    }

    public Request makeGetRequest(Key key,
                                  long tableId,
                                  Consistency consistency,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  LogContext lc) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final Get get = new Get(keyBytes, tableId);
        return makeReadRequest(get, partitionId, consistency, timeout,
                               timeoutUnit, lc);
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth)
        throws FaultException {

        return multiGet(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public SortedMap<Key, ValueVersion> multiGet(Key parentKey,
                                                 KeyRange subRange,
                                                 Depth depth,
                                                 Consistency consistency,
                                                 long timeout,
                                                 TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        /* Execute request. */
        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiGet get = new MultiGet(parentKeyBytes, subRange, depth);
        final Request req = makeReadRequest(get, partitionId, consistency,
                                            timeout, timeoutUnit, null);
        final Result result = executeRequest(req);

        /* Convert byte[] keys to Key objects. */
        final List<ResultKeyValueVersion> byteKeyResults =
            result.getKeyValueVersionList();
        final SortedMap<Key, ValueVersion> stringKeyResults =
            new TreeMap<Key, ValueVersion>();
        for (ResultKeyValueVersion entry : byteKeyResults) {
            stringKeyResults.put
                (keySerializer.fromByteArray(entry.getKeyBytes()),
                 new ValueVersion(entry.getValue(), entry.getVersion()));
        }
        assert result.getSuccess() == (!stringKeyResults.isEmpty());
        return stringKeyResults;
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth)
        throws FaultException {

        return multiGetKeys(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public SortedSet<Key> multiGetKeys(Key parentKey,
                                       KeyRange subRange,
                                       Depth depth,
                                       Consistency consistency,
                                       long timeout,
                                       TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        /* Execute request. */
        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiGetKeys get =
            new MultiGetKeys(parentKeyBytes, subRange, depth);
        final Request req = makeReadRequest(get, partitionId, consistency,
                                            timeout, timeoutUnit, null);
        final Result result = executeRequest(req);

        /* Convert byte[] keys to Key objects. */
        final List<ResultKey> byteKeyResults = result.getKeyList();
        final SortedSet<Key> stringKeySet = new TreeSet<Key>();
        for (ResultKey entry : byteKeyResults) {
            stringKeySet.add(keySerializer.fromByteArray(entry.getKeyBytes()));
        }
        assert result.getSuccess() == (!stringKeySet.isEmpty());
        return stringKeySet;
    }

    @Override
    public Iterator<KeyValueVersion> multiGetIterator(Direction direction,
                                                      int batchSize,
                                                      Key parentKey,
                                                      KeyRange subRange,
                                                      Depth depth)
        throws FaultException {

        return multiGetIterator(direction, batchSize, parentKey, subRange,
                                depth, null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion>
        multiGetIterator(final Direction direction,
                         final int batchSize,
                         final Key parentKey,
                         final KeyRange subRange,
                         final Depth depth,
                         final Consistency consistency,
                         final long timeout,
                         final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.REVERSE) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD and REVERSE are supported, got: " +
                 direction);
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);

        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);

        return new ArrayIterator<KeyValueVersion>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;

            @Override
            KeyValueVersion[] getMoreElements() {

                /* Avoid round trip if we know there are no more elements. */
                if (!moreElements) {
                    return null;
                }

                /* Execute request. */
                final MultiGetIterate get = new MultiGetIterate
                    (parentKeyBytes, subRange, useDepth, direction,
                     useBatchSize, resumeKey);
                final Request req = makeReadRequest
                    (get, partitionId, consistency, timeout,
                     timeoutUnit, null);
                final Result result = executeRequest(req);

                /* Get results and save resume key. */
                moreElements = result.hasMoreElements();
                final List<ResultKeyValueVersion> byteKeyResults =
                    result.getKeyValueVersionList();
                if (byteKeyResults.size() == 0) {
                    assert (!moreElements);
                    return null;
                }
                resumeKey = byteKeyResults.get
                    (byteKeyResults.size() - 1).getKeyBytes();

                /* Convert byte[] keys to Key objects. */
                final KeyValueVersion[] stringKeyResults =
                    new KeyValueVersion[byteKeyResults.size()];
                for (int i = 0; i < stringKeyResults.length; i += 1) {
                    final ResultKeyValueVersion entry = byteKeyResults.get(i);
                    stringKeyResults[i] = new KeyValueVersion
                        (keySerializer.fromByteArray(entry.getKeyBytes()),
                         entry.getValue(), entry.getVersion());
                }
                return stringKeyResults;
            }
        };
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(Direction direction,
                                              int batchSize,
                                              Key parentKey,
                                              KeyRange subRange,
                                              Depth depth)
        throws FaultException {

        return multiGetKeysIterator(direction, batchSize, parentKey, subRange,
                                    depth, null, 0, null);
    }

    @Override
    public Iterator<Key> multiGetKeysIterator(final Direction direction,
                                              final int batchSize,
                                              final Key parentKey,
                                              final KeyRange subRange,
                                              final Depth depth,
                                              final Consistency consistency,
                                              final long timeout,
                                              final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.REVERSE) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD and REVERSE are supported, got: " +
                 direction);
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);

        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);

        return new ArrayIterator<Key>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;

            @Override
            Key[] getMoreElements() {

                /* Avoid round trip if we know there are no more elements. */
                if (!moreElements) {
                    return null;
                }

                /* Execute request. */
                final MultiGetKeysIterate get = new MultiGetKeysIterate
                    (parentKeyBytes, subRange, useDepth, direction,
                     useBatchSize, resumeKey);
                final Request req = makeReadRequest
                    (get, partitionId, consistency,
                     timeout, timeoutUnit, null);
                final Result result = executeRequest(req);

                /* Get results and save resume key. */
                moreElements = result.hasMoreElements();
                final List<ResultKey> byteKeyResults = result.getKeyList();
                if (byteKeyResults.size() == 0) {
                    assert (!moreElements);
                    return null;
                }
                resumeKey = byteKeyResults.
                    get(byteKeyResults.size() - 1).getKeyBytes();

                /* Convert byte[] keys to Key objects. */
                final Key[] stringKeyResults = new Key[byteKeyResults.size()];
                for (int i = 0; i < stringKeyResults.length; i += 1) {
                    final byte[] entry = byteKeyResults.get(i).getKeyBytes();
                    stringKeyResults[i] = keySerializer.fromByteArray(entry);
                }
                return stringKeyResults;
            }
        };
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize)
        throws FaultException {

        return storeIterator(direction, batchSize, null, null, null,
                             null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion> storeIterator(Direction direction,
                                                   int batchSize,
                                                   Key parentKey,
                                                   KeyRange subRange,
                                                   Depth depth)
        throws FaultException {

        return storeIterator(direction, batchSize, parentKey, subRange, depth,
                             null, 0, null);
    }

    @Override
    public Iterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit)
        throws FaultException {

        return storeIterator(direction, batchSize, 1, nPartitions, parentKey,
                             subRange, depth, consistency, timeout,
                             timeoutUnit);
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit,
                      final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (storeIteratorConfig == null) {
            throw new IllegalArgumentException
                ("The StoreIteratorConfig argument must be supplied.");
        }

        return ParallelScan.
            createParallelScan(this, direction, batchSize, parentKey, subRange,
                               depth, consistency, timeout, timeoutUnit,
                               storeIteratorConfig);
    }

    /**
     * Internal use only.  Iterates using the same rules as storeIterator,
     * but over the single, given partition.
     *
     * @param direction the direction may be {@link Direction#FORWARD} or
     * {@link Direction#UNORDERED} though keys are always returned in forward
     * order for the given partition.  In the future we may support a faster,
     * unordered iteration.
     */
    public Iterator<KeyValueVersion>
        partitionIterator(final Direction direction,
                          final int batchSize,
                          final int partition,
                          final Key parentKey,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.FORWARD &&
            direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.FORWARD or Direction.UNORDERED is currently " +
                 "supported, got: " + direction);
        }

        return storeIterator(Direction.UNORDERED, batchSize, partition,
                             partition, parentKey, subRange, depth,
                             consistency, timeout, timeoutUnit);
    }

    private Iterator<KeyValueVersion>
        storeIterator(final Direction direction,
                      final int batchSize,
                      final int firstPartition,
                      final int lastPartition,
                      final Key parentKey,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes =
            (parentKey != null) ? keySerializer.toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            keySerializer.restrictRange(parentKey, subRange);

        return new ArrayIterator<KeyValueVersion>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;
            private PartitionId partitionId = new PartitionId(firstPartition);

            @Override
            KeyValueVersion[] getMoreElements() {

                while (true) {
                    /* If no more in one partition, move to the next. */
                    if ((!moreElements) &&
                        (partitionId.getPartitionId() < lastPartition)) {
                        partitionId =
                            new PartitionId(partitionId.getPartitionId() + 1);
                        moreElements = true;
                        resumeKey = null;
                    }

                    /* Avoid round trip when there are no more elements. */
                    if (!moreElements) {
                        return null;
                    }

                    /* Execute request. */
                    final StoreIterate get = new StoreIterate
                        (parentKeyBytes, useRange, useDepth, Direction.FORWARD,
                         useBatchSize, resumeKey);
                    final Request req = makeReadRequest
                        (get, partitionId, consistency,
                         timeout, timeoutUnit, null);
                    final Result result = executeRequest(req);

                    /* Get results and save resume key. */
                    moreElements = result.hasMoreElements();
                    final List<ResultKeyValueVersion> byteKeyResults =
                        result.getKeyValueVersionList();
                    if (byteKeyResults.size() == 0) {
                        assert (!moreElements);
                        continue;
                    }
                    resumeKey = byteKeyResults.get
                        (byteKeyResults.size() - 1).getKeyBytes();

                    /* Convert byte[] keys to Key objects. */
                    final KeyValueVersion[] stringKeyResults =
                        new KeyValueVersion[byteKeyResults.size()];
                    for (int i = 0; i < stringKeyResults.length; i += 1) {
                        final ResultKeyValueVersion entry =
                            byteKeyResults.get(i);
                        stringKeyResults[i] = createKeyValueVersion
                            (keySerializer.fromByteArray(entry.getKeyBytes()),
                             entry.getValue(), entry.getVersion(),
                             entry.getExpirationTime());
                    }
                    return stringKeyResults;
                }
            }
        };
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize)
        throws FaultException {

        return storeKeysIterator(direction, batchSize, null, null, null,
                                 null, 0, null);
    }

    @Override
    public Iterator<Key> storeKeysIterator(Direction direction,
                                           int batchSize,
                                           Key parentKey,
                                           KeyRange subRange,
                                           Depth depth)
        throws FaultException {

        return storeKeysIterator(direction, batchSize, parentKey, subRange,
                                 depth, null, 0, null);
    }

    @Override
    public Iterator<Key> storeKeysIterator(final Direction direction,
                                           final int batchSize,
                                           final Key parentKey,
                                           final KeyRange subRange,
                                           final Depth depth,
                                           final Consistency consistency,
                                           final long timeout,
                                           final TimeUnit timeoutUnit)
        throws FaultException {

        if (direction != Direction.UNORDERED) {
            throw new IllegalArgumentException
                ("Only Direction.UNORDERED is currently supported, got: " +
                 direction);
        }

        if ((parentKey != null) && (parentKey.getMinorPath().size()) > 0) {
            throw new IllegalArgumentException
                ("Minor path of parentKey must be empty");
        }

        final Depth useDepth =
            (depth != null) ? depth : Depth.PARENT_AND_DESCENDANTS;

        final int useBatchSize =
            (batchSize > 0) ? batchSize : DEFAULT_ITERATOR_BATCH_SIZE;

        final byte[] parentKeyBytes =
            (parentKey != null) ? keySerializer.toByteArray(parentKey) : null;

        /* Prohibit iteration of internal keyspace (//). */
        final KeyRange useRange =
            keySerializer.restrictRange(parentKey, subRange);

        return new ArrayIterator<Key>() {
            private boolean moreElements = true;
            private byte[] resumeKey = null;
            private PartitionId partitionId = new PartitionId(1);

            @Override
            Key[] getMoreElements() {

                while (true) {
                    /* If no more in one partition, move to the next. */
                    if ((!moreElements) &&
                        (partitionId.getPartitionId() < nPartitions)) {
                        partitionId =
                            new PartitionId(partitionId.getPartitionId() + 1);
                        moreElements = true;
                        resumeKey = null;
                    }

                    /* Avoid round trip when there are no more elements. */
                    if (!moreElements) {
                        return null;
                    }

                    /* Execute request. */
                    final StoreKeysIterate get = new StoreKeysIterate
                        (parentKeyBytes, useRange, useDepth, Direction.FORWARD,
                         useBatchSize, resumeKey);
                    final Request req = makeReadRequest
                        (get, partitionId, consistency,
                         timeout, timeoutUnit, null);
                    final Result result = executeRequest(req);

                    /* Get results and save resume key. */
                    moreElements = result.hasMoreElements();
                    final List<ResultKey> byteKeyResults = result.getKeyList();
                    if (byteKeyResults.size() == 0) {
                        assert (!moreElements);
                        continue;
                    }
                    resumeKey = byteKeyResults.
                        get(byteKeyResults.size() - 1).getKeyBytes();

                    /* Convert byte[] keys to Key objects. */
                    final Key[] stringKeyResults =
                        new Key[byteKeyResults.size()];
                    for (int i = 0; i < stringKeyResults.length; i += 1) {
                        final byte[] entry =
                            byteKeyResults.get(i).getKeyBytes();
                        stringKeyResults[i] =
                            keySerializer.fromByteArray(entry);
                    }
                    return stringKeyResults;
                }
            }
        };
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(final Direction direction,
                          final int batchSize,
                          final Key parentKey,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit,
                          final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (storeIteratorConfig == null) {
            throw new IllegalArgumentException
                ("The StoreIteratorConfig argument must be supplied.");
        }

        return ParallelScan.
            createParallelKeyScan(this, direction, batchSize,
                                  parentKey, subRange,
                                  depth, consistency, timeout, timeoutUnit,
                                  storeIteratorConfig);
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(final Iterator<Key> parentKeyiterator,
                      final int batchSize,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit,
                      final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyiterator == null) {
            throw new IllegalArgumentException("The parent key iterator " +
                                               "argument should not be null.");
        }

        final List<Iterator<Key>> parentKeyiterators =
            Arrays.asList(parentKeyiterator);

        return storeIterator(parentKeyiterators, batchSize, subRange,
                             depth, consistency, timeout, timeoutUnit,
                             storeIteratorConfig);
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(final Iterator<Key> parentKeyiterator,
                          final int batchSize,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit,
                          final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyiterator == null) {
            throw new IllegalArgumentException("The parent key iterator " +
                                               "argument should not be null.");
        }

        final List<Iterator<Key>> parentKeyiterators =
            Arrays.asList(parentKeyiterator);

        return storeKeysIterator(parentKeyiterators, batchSize, subRange,
                                 depth, consistency, timeout, timeoutUnit,
                                 storeIteratorConfig);
    }

    @Override
    public ParallelScanIterator<KeyValueVersion>
        storeIterator(final List<Iterator<Key>> parentKeyIterators,
                      final int batchSize,
                      final KeyRange subRange,
                      final Depth depth,
                      final Consistency consistency,
                      final long timeout,
                      final TimeUnit timeoutUnit,
                      final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyIterators == null || parentKeyIterators.isEmpty()) {
            throw new IllegalArgumentException("The key iterator list cannot " +
                "be null or empty.");
        }

        if (parentKeyIterators.contains(null)) {
            throw new IllegalArgumentException("Elements of key iterator " +
                "list must not be null.");
        }

        return BulkMultiGet.createBulkMultiGetIterator(this, parentKeyIterators,
                                                       batchSize, subRange,
                                                       depth, consistency,
                                                       timeout, timeoutUnit,
                                                       storeIteratorConfig);
    }

    @Override
    public ParallelScanIterator<Key>
        storeKeysIterator(final List<Iterator<Key>> parentKeyIterators,
                          final int batchSize,
                          final KeyRange subRange,
                          final Depth depth,
                          final Consistency consistency,
                          final long timeout,
                          final TimeUnit timeoutUnit,
                          final StoreIteratorConfig storeIteratorConfig)
        throws FaultException {

        if (parentKeyIterators == null || parentKeyIterators.isEmpty()) {
            throw new IllegalArgumentException("The key iterator list cannot " +
                "be null or empty.");
        }

        if (parentKeyIterators.contains(null)) {
            throw new IllegalArgumentException("Elements of key iterator " +
                "must not be null.");
        }

        return BulkMultiGet.createBulkMultiGetKeysIterator(this,
                                                           parentKeyIterators,
                                                           batchSize, subRange,
                                                           depth, consistency,
                                                           timeout, timeoutUnit,
                                                           storeIteratorConfig);
    }

    private abstract class ArrayIterator<E> implements Iterator<E> {

        private E[] elements = null;
        private int nextElement = 0;

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (elements != null && nextElement < elements.length) {
                return true;
            }
            elements = getMoreElements();
            if (elements == null) {
                return false;
            }
            assert (elements.length > 0);
            nextElement = 0;
            return true;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return elements[nextElement++];
        }

        /**
         * Returns more elements or null if there are none.  May not return a
         * zero length array.
         */
        abstract E[] getMoreElements();
    }

    @Override
    public Version put(Key key,
                       Value value)
        throws FaultException {

        return put(key, value, null, null, 0, null);
    }

    @Override
    public Version put(Key key,
                       Value value,
                       ReturnValueVersion prevValue,
                       Durability durability,
                       long timeout,
                       TimeUnit timeoutUnit)
        throws FaultException {

        return putInternal(key, value, prevValue, 0,
                           durability, timeout, timeoutUnit);
    }

    public Version putInternal(Key key,
                               Value value,
                               ReturnValueVersion prevValue,
                               long tableId,
                               Durability durability,
                               long timeout,
                               TimeUnit timeoutUnit)

        throws FaultException {

        final Result result = putInternalResult(key,
                                                value,
                                                prevValue,
                                                tableId,
                                                durability,
                                                timeout,
                                                timeoutUnit,
                                                null, false, null);

        return getPutResult(result);
    }

    public static Version getPutResult(Result result) {
        assert result.getSuccess() == (result.getNewVersion() != null);
        return result.getNewVersion();
    }

    public Result putInternalResult(Key key,
                                    Value value,
                                    ReturnValueVersion prevValue,
                                    long tableId,
                                    Durability durability,
                                    long timeout,
                                    TimeUnit timeoutUnit,
                                    TimeToLive ttl,
                                    boolean updateTTL,
                                    LogContext lc)
        throws FaultException {

        final Request req = makePutRequest(key, value, prevValue, tableId,
                                           durability, timeout, timeoutUnit,
                                           ttl, updateTTL, lc);
        return executeRequestWithPrev(req, prevValue);
    }

    public Request makePutRequest(Key key,
                                  Value value,
                                  ReturnValueVersion prevValue,
                                  long tableId,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  TimeToLive ttl,
                                  boolean updateTTL,
                                  LogContext lc) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put = new Put(keyBytes, value, prevValChoice, tableId,
                ttl, updateTTL);
        return makeWriteRequest(put, partitionId, durability, timeout,
                                timeoutUnit, lc);
    }

    public Result executeRequestWithPrev(Request req,
                                         ReturnValueVersion prevValue)
        throws FaultException {

        final Result result = executeRequest(req);
        return resultSetPreviousValue(result, prevValue);
    }

    public static Result resultSetPreviousValue(Result result,
                                                ReturnValueVersion prevValue) {
        if (prevValue != null) {
            prevValue.setValue(result.getPreviousValue());
            prevValue.setVersion(result.getPreviousVersion());
        }
        return result;
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value)
        throws FaultException {

        return putIfAbsent(key, value, null, null, 0, null);
    }

    @Override
    public Version putIfAbsent(Key key,
                               Value value,
                               ReturnValueVersion prevValue,
                               Durability durability,
                               long timeout,
                               TimeUnit timeoutUnit)
        throws FaultException {

        return putIfAbsentInternal(key, value, prevValue, 0,
                                   durability, timeout, timeoutUnit);
    }

    public Version putIfAbsentInternal(Key key,
            Value value,
            ReturnValueVersion prevValue,
            long tableId,
            Durability durability,
            long timeout,
            TimeUnit timeoutUnit) {

        final Result result = putIfAbsentInternalResult(key,
                                                        value,
                                                        prevValue,
                                                        tableId,
                                                        durability,
                                                        timeout,
                                                        timeoutUnit,
                                                        null,
                                                        false,
                                                        null);
        return getPutResult(result);
    }

    public Result putIfAbsentInternalResult(Key key,
                                            Value value,
                                            ReturnValueVersion prevValue,
                                            long tableId,
                                            Durability durability,
                                            long timeout,
                                            TimeUnit timeoutUnit,
                                            TimeToLive ttl,
                                            boolean updateTTL,
                                            LogContext lc)
        throws FaultException {

        final Request req = makePutIfAbsentRequest(key, value, prevValue,
                                                   tableId, durability,
                                                   timeout, timeoutUnit,
                                                   ttl, updateTTL, lc);
        return executeRequestWithPrev(req, prevValue);
    }

    public Request makePutIfAbsentRequest(Key key,
                                          Value value,
                                          ReturnValueVersion prevValue,
                                          long tableId,
                                          Durability durability,
                                          long timeout,
                                          TimeUnit timeoutUnit,
                                          TimeToLive ttl,
                                          boolean updateTTL,
                                          LogContext lc) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put =
            new PutIfAbsent(keyBytes, value, prevValChoice, tableId,
                    ttl, updateTTL);
        return makeWriteRequest(put, partitionId, durability, timeout,
                                timeoutUnit, lc);
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value)
        throws FaultException {

        return putIfPresent(key, value, null, null, 0, null);
    }

    @Override
    public Version putIfPresent(Key key,
                                Value value,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit)
        throws FaultException {

        final Result result = putIfPresentInternalResult(key,
                                                         value,
                                                         prevValue,
                                                         0,
                                                         durability,
                                                         timeout,
                                                         timeoutUnit,
                                                         null,
                                                         false,
                                                         null);

        return getPutResult(result);
    }

    public Result putIfPresentInternalResult(Key key,
                                             Value value,
                                             ReturnValueVersion prevValue,
                                             long tableId,
                                             Durability durability,
                                             long timeout,
                                             TimeUnit timeoutUnit,
                                             TimeToLive ttl,
                                             boolean updateTTL,
                                             LogContext lc)
        throws FaultException {

        final Request req = makePutIfPresentRequest(key, value, prevValue,
                                                    tableId, durability,
                                                    timeout, timeoutUnit,
                                                    ttl, updateTTL, lc);
        return executeRequestWithPrev(req, prevValue);
    }

    public Request makePutIfPresentRequest(Key key,
                                           Value value,
                                           ReturnValueVersion prevValue,
                                           long tableId,
                                           Durability durability,
                                           long timeout,
                                           TimeUnit timeoutUnit,
                                           TimeToLive ttl,
                                           boolean updateTTL,
                                           LogContext lc) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put =
            new PutIfPresent(keyBytes, value, prevValChoice, tableId,
                    ttl, updateTTL);
        return makeWriteRequest(put, partitionId, durability, timeout,
                                timeoutUnit, lc);
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion)
        throws FaultException {

        return putIfVersion(key, value, matchVersion, null, null, 0, null);
    }

    @Override
    public Version putIfVersion(Key key,
                                Value value,
                                Version matchVersion,
                                ReturnValueVersion prevValue,
                                Durability durability,
                                long timeout,
                                TimeUnit timeoutUnit)
        throws FaultException {

        return putIfVersionInternal(key, value, matchVersion, prevValue, 0,
                                    durability, timeout, timeoutUnit);
    }

    public Version putIfVersionInternal(Key key,
            Value value,
            Version matchVersion,
            ReturnValueVersion prevValue,
            long tableId,
            Durability durability,
            long timeout,
            TimeUnit timeoutUnit)
        throws FaultException {

        final Result result = putIfVersionInternalResult(key,
                                                         value,
                                                         matchVersion,
                                                         prevValue,
                                                         tableId,
                                                         durability,
                                                         timeout,
                                                         timeoutUnit,
                                                         null,
                                                         false,
                                                         null);

        return getPutResult(result);
    }

    public Result putIfVersionInternalResult(Key key,
                                             Value value,
                                             Version matchVersion,
                                             ReturnValueVersion prevValue,
                                             long tableId,
                                             Durability durability,
                                             long timeout,
                                             TimeUnit timeoutUnit,
                                             TimeToLive ttl,
                                             boolean updateTTL,
                                             LogContext lc)
        throws FaultException {

        final Request req = makePutIfVersionRequest(key, value, matchVersion,
                                                    prevValue, tableId,
                                                    durability, timeout,
                                                    timeoutUnit, ttl,
                                                    updateTTL, lc);

        return executeRequestWithPrev(req, prevValue);
    }

    public Request makePutIfVersionRequest(Key key,
                                           Value value,
                                           Version matchVersion,
                                           ReturnValueVersion prevValue,
                                           long tableId,
                                           Durability durability,
                                           long timeout,
                                           TimeUnit timeoutUnit,
                                           TimeToLive ttl,
                                           boolean updateTTL,
                                           LogContext lc) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Put put = new PutIfVersion(keyBytes, value, prevValChoice,
                                         matchVersion, tableId,
                                         ttl, updateTTL);
        return makeWriteRequest(put, partitionId, durability, timeout,
                                timeoutUnit, lc);
    }

    @Override
    public boolean delete(Key key)
        throws FaultException {

        return delete(key, null, null, 0, null);
    }

    @Override
    public boolean delete(Key key,
                          ReturnValueVersion prevValue,
                          Durability durability,
                          long timeout,
                          TimeUnit timeoutUnit)
        throws FaultException {

        return deleteInternal(key, prevValue, durability,
                              timeout, timeoutUnit, 0);
    }

    public boolean deleteInternal(Key key,
                                  ReturnValueVersion prevValue,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  long tableId)
        throws FaultException {

        return deleteInternalResult(key,
                                    prevValue,
                                    durability,
                                    timeout,
                                    timeoutUnit,
                                    tableId, null).getSuccess();
    }

    public Result deleteInternalResult(Key key,
                                       ReturnValueVersion prevValue,
                                       Durability durability,
                                       long timeout,
                                       TimeUnit timeoutUnit,
                                       long tableId,
                                       LogContext lc)
        throws FaultException {

        final Request req = makeDeleteRequest(key, prevValue, durability,
                                              timeout, timeoutUnit, tableId, lc);
        return executeRequestWithPrev(req, prevValue);
    }

    public Request makeDeleteRequest(Key key,
                                     ReturnValueVersion prevValue,
                                     Durability durability,
                                     long timeout,
                                     TimeUnit timeoutUnit,
                                     long tableId,
                                     LogContext lc) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Delete del = new Delete(keyBytes, prevValChoice, tableId);
        return makeWriteRequest(del, partitionId, durability, timeout,
                                timeoutUnit, lc);
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion)
        throws FaultException {

        return deleteIfVersion(key, matchVersion, null, null, 0, null);
    }

    @Override
    public boolean deleteIfVersion(Key key,
                                   Version matchVersion,
                                   ReturnValueVersion prevValue,
                                   Durability durability,
                                   long timeout,
                                   TimeUnit timeoutUnit)
        throws FaultException {

        return deleteIfVersionInternal(key, matchVersion, prevValue,
                                       durability, timeout, timeoutUnit, 0);
    }

    public boolean deleteIfVersionInternal(Key key,
                                           Version matchVersion,
                                           ReturnValueVersion prevValue,
                                           Durability durability,
                                           long timeout,
                                           TimeUnit timeoutUnit,
                                           long tableId)
        throws FaultException {

        return deleteIfVersionInternalResult(key,
                                             matchVersion,
                                             prevValue,
                                             durability,
                                             timeout,
                                             timeoutUnit,
                                             tableId,
                                             null).getSuccess();
    }

    public Result deleteIfVersionInternalResult(Key key,
                                                Version matchVersion,
                                                ReturnValueVersion prevValue,
                                                Durability durability,
                                                long timeout,
                                                TimeUnit timeoutUnit,
                                                long tableId,
                                                LogContext lc)
        throws FaultException {

        final Request req = makeDeleteIfVersionRequest(key, matchVersion,
                                                       prevValue, durability,
                                                       timeout, timeoutUnit,
                                                       tableId, lc);
        return executeRequestWithPrev(req, prevValue);
    }

    public Request makeDeleteIfVersionRequest(Key key,
                                              Version matchVersion,
                                              ReturnValueVersion prevValue,
                                              Durability durability,
                                              long timeout,
                                              TimeUnit timeoutUnit,
                                              long tableId,
                                              LogContext lc) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        final PartitionId partitionId = dispatcher.getPartitionId(keyBytes);
        final ReturnValueVersion.Choice prevValChoice = (prevValue != null) ?
            prevValue.getReturnChoice() :
            ReturnValueVersion.Choice.NONE;
        final Delete del = new DeleteIfVersion(keyBytes, prevValChoice,
                                               matchVersion, tableId);
        return makeWriteRequest(del, partitionId, durability, timeout,
                                timeoutUnit, lc);
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth)
        throws FaultException {

        return multiDelete(parentKey, subRange, depth, null, 0, null);
    }

    @Override
    public int multiDelete(Key parentKey,
                           KeyRange subRange,
                           Depth depth,
                           Durability durability,
                           long timeout,
                           TimeUnit timeoutUnit)
        throws FaultException {

        if (depth == null) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        }

        final byte[] parentKeyBytes = keySerializer.toByteArray(parentKey);
        final PartitionId partitionId =
            dispatcher.getPartitionId(parentKeyBytes);
        final MultiDelete del =
            new MultiDelete(parentKeyBytes, subRange,
                            depth,
                            largeObjectImpl.getLOBSuffixBytes());
        final Request req = makeWriteRequest(del, partitionId, durability,
                                             timeout, timeoutUnit, null);
        final Result result = executeRequest(req);
        return result.getNDeletions();
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations)
        throws OperationExecutionException,
               FaultException {

        return execute(operations, null, 0, null);
    }

    @Override
    public List<OperationResult> execute(List<Operation> operations,
                                         Durability durability,
                                         long timeout,
                                         TimeUnit timeoutUnit)
        throws OperationExecutionException,
               FaultException {

        Result result = executeInternal(operations, 0, durability,
                                        timeout, timeoutUnit, null);
        return result.getExecuteResult();
    }

    /**
     * Internal use only
     *
     * Public for use by cloud proxy
     */
    public Result executeInternal(List<Operation> operations,
                                  long tableId,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit,
                                  LogContext lc)
        throws OperationExecutionException,
               FaultException {

        final Request req = makeExecuteRequest(operations, tableId, durability,
                                               timeout, timeoutUnit, lc);
        return processExecuteResult(executeRequest(req), operations);
    }

    public Request makeExecuteRequest(List<Operation> operations,
                                      long tableId,
                                      Durability durability,
                                      long timeout,
                                      TimeUnit timeoutUnit,
                                      LogContext lc) {
        /* Validate operations. */
        final List<OperationImpl> ops = OperationImpl.downcast(operations);
        if (ops == null || ops.size() == 0) {
            throw new IllegalArgumentException
                ("operations must be non-null and non-empty");
        }
        final OperationImpl firstOp = ops.get(0);
        final List<String> firstMajorPath = firstOp.getKey().getMajorPath();
        final Set<Key> keySet = new HashSet<Key>();
        keySet.add(firstOp.getKey());
        checkLOBKeySuffix(firstOp.getInternalOp());
        for (int i = 1; i < ops.size(); i += 1) {
            final OperationImpl op = ops.get(i);
            final Key opKey = op.getKey();
            if (!opKey.getMajorPath().equals(firstMajorPath)) {
                throw new IllegalArgumentException
                    ("Two operations have different major paths, first: " +
                     firstOp.getKey() + " other: " + opKey);
            }
            if (keySet.add(opKey) == false) {
                throw new IllegalArgumentException
                    ("More than one operation has the same Key: " + opKey);
            }
            checkLOBKeySuffix(op.getInternalOp());
        }

        /* Execute the execute. */
        final PartitionId partitionId =
            dispatcher.getPartitionId(firstOp.getInternalOp().getKeyBytes());
        final Execute exe = new Execute(ops, tableId);
        return makeWriteRequest(exe, partitionId, durability, timeout,
                                timeoutUnit, lc);
    }

    public static Result processExecuteResult(Result result,
                                              List<Operation> operations)
        throws OperationExecutionException {

        final OperationExecutionException exception =
            result.getExecuteException(operations);
        if (exception != null) {
            throw exception;
        }
        return result;
    }

    @Override
    public void put(List<EntryStream<KeyValue>> kvStreams,
                    BulkWriteOptions writeOptions) {

        if (kvStreams == null || kvStreams.isEmpty()) {
            throw new IllegalArgumentException("The stream list cannot be " +
                "null or empty.");
        }

        if (kvStreams.contains(null)) {
            throw new IllegalArgumentException("Elements of stream list " +
                "must not be null.");
        }

        final BulkWriteOptions options =
            (writeOptions != null) ? writeOptions : new BulkWriteOptions();

        final BulkPut<KeyValue> bulkPut =
            new BulkPut<KeyValue>(this, options, kvStreams, getLogger()) {

                @Override
                public BulkPut<KeyValue>.StreamReader<KeyValue>
                    createReader(int streamId, EntryStream<KeyValue> stream) {

                    return new StreamReader<KeyValue>(streamId, stream) {

                        @Override
                        protected Key getKey(KeyValue kv) {
                            return kv.getKey();
                        }

                        @Override
                        protected Value getValue(KeyValue kv) {
                            return kv.getValue();
                        }
                    };
                }

                @Override
                protected KeyValue convertToEntry(Key key, Value value) {
                    return new KeyValue(key, value);
                }
        };

        try {
            bulkPut.execute();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt during " +
                    "putBulk()", e);
        }
    }

    /*
     * Internal method to put KeyValueVersion, which may contain expiration
     * time, to be translated into a TTL. This is used by import.
     *
     * @param referenceTime the reference time to be subtracted from the
     * expiration time (if non-zero) on any KeyValueVersion to generate a TTL
     * duration. This value must be less than any expiration time to avoid
     * negative durations.
     */
    public void put(List<EntryStream<KeyValueVersion>> kvStreams,
                    final long referenceTime,
                    BulkWriteOptions writeOptions) {

        if (kvStreams == null || kvStreams.isEmpty()) {
            throw new IllegalArgumentException("The stream list cannot be " +
                "null or empty.");
        }

        if (kvStreams.contains(null)) {
            throw new IllegalArgumentException("Elements of stream list " +
                "must not be null.");
        }

        final BulkWriteOptions options =
            (writeOptions != null) ? writeOptions : new BulkWriteOptions();

        final BulkPut<KeyValueVersion> bulkPut =
            new BulkPut<KeyValueVersion>(this, options, kvStreams, getLogger()) {

                @Override
                public BulkPut<KeyValueVersion>.StreamReader<KeyValueVersion>
                    createReader(int streamId, EntryStream<KeyValueVersion> stream) {

                    return new StreamReader<KeyValueVersion>(streamId, stream) {

                        @Override
                        protected Key getKey(KeyValueVersion kv) {
                            return kv.getKey();
                        }

                        @Override
                        protected Value getValue(KeyValueVersion kv) {
                            return kv.getValue();
                        }

                        @Override
                        protected TimeToLive getTTL(KeyValueVersion kv) {
                            long expirationTime = kv.getExpirationTime();
                            if (expirationTime == 0) {
                                return null;
                            }
                            if (referenceTime > expirationTime) {
                                throw new IllegalArgumentException(
                                    "Reference time must be less than " +
                                    "expiration time");
                            }
                            return TimeToLive.fromExpirationTime(
                                kv.getExpirationTime(), referenceTime);
                        }
                    };
                }

                @Override
                protected KeyValueVersion convertToEntry(Key key, Value value) {
                    return new KeyValueVersion(key, value);
                }
        };

        try {
            bulkPut.execute();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unexpected interrupt during " +
                    "putBulk()", e);
        }
    }

    /**
     * TODO: Support to configure the checkpoint delay time.
     * Sam said that a large part of JE overhead (as much as 20% of throughput)
     * is in the resources of taking JE checkpoints at regular intervals. It
     * would boost load performance in certain circumstances, for example, when
     * a store was being populated initially (there were no other ongoing
     * operations), to delay taking a checkpoint on each RN until the very end
     * of the bulk load.
     *
     * Doing so requires support in both JE and JE HA, which does not exist
     * currently.
     */
    public List<Integer> putBatch(PartitionId partitionId,
                                  List<KVPair> le,
                                  long[] tableIds,
                                  Durability durability,
                                  long timeout,
                                  TimeUnit timeoutUnit)
        throws FaultException {

        PutBatch pb = new PutBatch(le, tableIds);

        final Request req = makeWriteRequest(pb, partitionId, durability,
                                             timeout, timeoutUnit, null);
        final Result.PutBatchResult result =
            (Result.PutBatchResult)executeRequest(req);

        List<Integer> keysPresent = result.getKeysPresent() ;

        return keysPresent;
    }

    @Override
    public OperationFactoryImpl getOperationFactory() {
        return operationFactory;
    }

    @Override
    public void close() {
        synchronized(loginLock) {
            if (loginMgr != null) {
                logout();
            }
        }
        dispatcher.shutdown(null);
        sharedThreadPool.shutdownNow();
        /* Set the close flag after successfully close */
        isClosed = true;
    }

    public Request makeWriteRequest(InternalOperation op,
                                     PartitionId partitionId,
                                     Durability durability,
                                     long timeout,
                                     TimeUnit timeoutUnit,
                                     LogContext lc) {
        checkLOBKeySuffix(op);

        return makeRequest
            (op, partitionId, null /*repGroupId*/, true /*write*/,
             ((durability != null) ? durability : defaultDurability),
             null /*consistency*/, timeout, timeoutUnit, lc);
    }

    /**
     * Perform any LOB key specific checks when using non-internal handles.
     *
     * @param op the operation whose key is to be checked
     */
    private void checkLOBKeySuffix(InternalOperation op) {

        if (isInternalHandle()) {
            return;
        }

        final byte[] keyBytes =
            op.checkLOBSuffix(largeObjectImpl.getLOBSuffixBytes());

        if (keyBytes == null) {
            return;
        }

        final String msg =
            "Operation: " + op.getOpCode() +
            " Illegal LOB key argument: " +
            Key.fromByteArray(keyBytes) +
            ". Use LOB-specific APIs to modify a LOB key/value pair.";

        throw new IllegalArgumentException(msg);
    }

    public Request makeReadRequest(InternalOperation op,
                                   PartitionId partitionId,
                                   Consistency consistency,
                                   long timeout,
                                   TimeUnit timeoutUnit,
                                   LogContext lc) {

        return makeRequest
            (op, partitionId, null /*repGroupId*/,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             timeout, timeoutUnit, lc);
    }

    public Request makeReadRequest(InternalOperation op,
                                   RepGroupId repGroupId,
                                   Consistency consistency,
                                   long timeout,
                                   TimeUnit timeoutUnit,
                                   LogContext lc) {
        return makeRequest
            (op, null /*partitionId*/, repGroupId,
             false /*write*/, null /*durability*/,
             ((consistency != null) ? consistency : defaultConsistency),
             timeout, timeoutUnit, lc);
    }

    private Request makeRequest(InternalOperation op,
                                PartitionId partitionId,
                                RepGroupId repGroupId,
                                boolean write,
                                Durability durability,
                                Consistency consistency,
                                long timeout,
                                TimeUnit timeoutUnit,
                                LogContext lc) {

        int requestTimeoutMs = defaultRequestTimeoutMs;
        if (timeout > 0) {
            requestTimeoutMs = PropUtil.durationToMillis(timeout, timeoutUnit);
            if (requestTimeoutMs > readTimeoutMs) {
                String format = "Request timeout parameter: %,d ms exceeds " +
                    "socket read timeout: %,d ms";
                throw new IllegalArgumentException
                    (String.format(format, requestTimeoutMs, readTimeoutMs));
            }
        }

        final Topology topology = getTopology();
        final int topoSeqNumber =
            (topology == null) ? 0 : topology.getSequenceNumber();

        return (partitionId != null) ?
            new Request
            (op, partitionId, write, durability, consistency, DEFAULT_TTL,
             topoSeqNumber,
             dispatcher.getDispatcherId(), requestTimeoutMs,
             !write ? dispatcher.getReadZoneIds() : null, lc) :
            new Request
            (op, repGroupId, write, durability, consistency, DEFAULT_TTL,
             topoSeqNumber,
             dispatcher.getDispatcherId(), requestTimeoutMs,
             !write ? dispatcher.getReadZoneIds() : null, lc);
    }

    /**
     * Invokes a request through the request handler
     *
     * @param request the request to run
     * @return the result of the request
     * @see #executeRequest(Request, ResultHandler)
     */
    public Result executeRequest(Request request)
        throws FaultException {

        try {
            return getExecuteResult(executeRequestInternal(request));
        } catch (AuthenticationFailureException afe) {
            if (afe.getCause() instanceof SSLHandshakeException) {

                tryResolveSSLHandshakeError();
                return getExecuteResult(executeRequestInternal(request));
            }
            throw afe;
        } catch (MetadataNotFoundException mnfe) {
            tableAPI.metadataNotification(mnfe.getTableMetadataSeqNum());
            throw mnfe;
        }
    }

    private Result getExecuteResult(Response response) {
        final Result result = response.getResult();
        if (result.getMetadataSeqNum() > 0) {
            tableAPI.metadataNotification(result.getMetadataSeqNum());
        }
        assert TestHookExecute.doHookIfSet(executeRequestHook, result);
        return result;
    }

    /**
     * A hook called by executeRequestInternal() after the execution of request,
     * the returning Result object is passed into the hook.
     */
    private TestHook<Result> executeRequestHook;

    private Response executeRequestInternal(Request request)
        throws FaultException {

        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return dispatcher.execute(request, loginMgr);
        } catch (AuthenticationRequiredException are) {
            if (!tryReauthenticate(requestLoginMgr)){
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return dispatcher.execute(request, loginMgr);
        }
    }

    /**
     * For testing.
     */
    public void setExecuteRequestHook(TestHook<Result> executeRequestHook) {
        this.executeRequestHook = executeRequestHook;
    }

    /**
     * Invokes a request through the request handler, returning the result
     * asynchronously.
     *
     * @param request the request to run
     * @param handler the result handler
     * @see #executeRequest(Request)
     */
    public void executeRequest(final Request request,
                               final ResultHandler<Result> handler) {
        class ExecuteRequestHandler implements ResultHandler<Response> {

            /* Flags to make sure we do these things only once */
            private volatile boolean didReauth;
            private volatile boolean didResolveSSL;

            private volatile LoginManager requestLoginMgr;
            void executeInternal() {
                requestLoginMgr = loginMgr;
                dispatcher.execute(request, null, loginMgr, this);
            }
            @Override
            public void onResult(Response response, final Throwable e) {
                if (!didReauth &&
                    (e instanceof AuthenticationRequiredException)) {
                    didReauth = true;
                    class ReauthenticateAsync implements Runnable {
                        @Override
                        public void run() {
                            if (tryReauthenticate(requestLoginMgr)) {
                                executeInternal();
                            } else {
                                handler.onResult(null, e);
                            }
                        }
                    }
                    AsyncRegistryUtils.getEndpointGroup()
                        .getSchedExecService()
                        .schedule(new ReauthenticateAsync(), 0, null);
                    return;
                }
                if (!didResolveSSL &&
                    (e instanceof AuthenticationFailureException) &&
                    (e.getCause() instanceof SSLHandshakeException)) {
                    didResolveSSL = true;
                    tryResolveSSLHandshakeError();
                    executeInternal();
                    return;
                }
                if (e instanceof MetadataNotFoundException) {
                    final MetadataNotFoundException mnfe =
                        (MetadataNotFoundException) e;
                    tableAPI.metadataNotification(
                        mnfe.getTableMetadataSeqNum());
                }
                if (e != null) {
                    handler.onResult(null, e);
                } else {
                    handler.onResult(getExecuteResult(response), null);
                }
            }
        }
        new ExecuteRequestHandler().executeInternal();
    }

    /* (non-Javadoc)
     * @see oracle.kv.KVStore#getStats(com.sleepycat.je.StatsConfig)
     */
    @Override
    public KVStats getStats(boolean clear) {
        return new KVStats(clear, dispatcher);
    }

    @SuppressWarnings("deprecation")
    @Override
    public oracle.kv.avro.AvroCatalog getAvroCatalog() {

        /* First check for an existing catalog without any synchronization. */
        oracle.kv.avro.AvroCatalog catalog = avroCatalogRef.get();
        if (catalog != null) {
            return catalog;
        }

        /*
         * Catalog creation is fairly expensive because it queries all schemas
         * from the store. We use synchronization (rather than compareAndSet)
         * to avoid the potential cost of creating two or more catalogs if
         * multiple threads initially call this method concurrently.
         *
         * Note that if there are multiple handles (created via the copy
         * constructor) they will share the same AtomicReference instance. So
         * multiple threads will always synchronize on the same instance.
         */
        synchronized (avroCatalogRef) {

            /*
             * Return catalog if another thread created it while we waited to
             * get the mutex.  The double-check is safe because the
             * AtomicReference is effectively volatile.
             */
            catalog = avroCatalogRef.get();
            if (catalog != null) {
                return catalog;
            }

            /*
             * Create the catalog and update the AtomicReference while
             * synchronized.
             */
            catalog = new AvroCatalogImpl(this);
            avroCatalogRef.set(catalog);
            return catalog;
        }
    }

    @Override
    public void login(LoginCredentials creds)
        throws RequestTimeoutException, AuthenticationFailureException,
               FaultException {

        if (creds == null) {
            throw new IllegalArgumentException("No credentials provided");
        }

        final LoginManager priorLoginMgr;
        synchronized (loginLock) {
            /*
             * If there is an existing login, the new creds must be for the
             * same username.
             */
            if (loginMgr != null) {
                if ((loginMgr.getUsername() == null &&
                     creds.getUsername() != null) ||
                    (loginMgr.getUsername() != null &&
                     !loginMgr.getUsername().equals(creds.getUsername()))) {
                    throw new AuthenticationFailureException(
                        "Logout required prior to logging in with new " +
                        "user identity.");
                }
            }

            final RepNodeLoginManager rnlm =
                new RepNodeLoginManager(creds.getUsername(), true);
            rnlm.setTopology(dispatcher.getTopologyManager());

            if (creds instanceof KerberosCredentials) {
                final KerberosClientCreds clientCreds = KVStoreLogin.
                    getKrbClientCredentials((KerberosCredentials) creds);
                rnlm.login(clientCreds);
            } else {
                rnlm.login(creds);
            }

            if (creds instanceof KerberosCredentials) {
                /*
                 * If login with Kerberos credentials, refresh cached
                 * Kerberos principals info based on topology.
                 */
                try {
                    rnlm.locateKrbPrincipals();
                } catch (KVStoreException e) {
                    throw new FaultException(e, false /*isRemote*/);
                }
            }

            /* login succeeded - establish new login */
            priorLoginMgr = loginMgr;
            if (isDispatcherOwner) {
                dispatcher.setRegUtilsLoginManager(rnlm);
            }
            this.loginMgr = rnlm;
            largeObjectImpl.renewLoginMgr(this.loginMgr);
            if (statementExecutor != null) {
                statementExecutor.renewLoginManager(this.loginMgr);
            }
        }

        if (priorLoginMgr != null) {
            Exception logException = null;
            try {
                priorLoginMgr.logout();
            } catch (SessionAccessException re) {
                /* ok */
                logException = re;
            } catch (AuthenticationRequiredException are) {
                /* ok */
                logException = are;
            }

            if (logException != null) {
                logger.info(logException.getMessage());
            }
        }
    }

    @Override
    public void logout()
        throws RequestTimeoutException, FaultException {

        synchronized(loginLock) {
            if (loginMgr == null) {
                throw new AuthenticationRequiredException(
                    "The KVStore handle has no associated login",
                    false /* isReturnSignal */);
            }

            try {
                loginMgr.logout();
            } catch (SessionAccessException sae) {
                logger.fine(sae.getMessage());
                /* ok */
            } finally {
                if (isDispatcherOwner) {
                    dispatcher.setRegUtilsLoginManager(null);
                }
            }
        }
    }

    /** For testing. */
    public boolean isAvroCatalogPopulated() {
        return (avroCatalogRef.get() != null);
    }

    /**
     * For unit test and debugging assistance.
     */
    public PartitionId getPartitionId(Key key) {
        final byte[] keyBytes = keySerializer.toByteArray(key);
        return dispatcher.getPartitionId(keyBytes);
    }

    public long getDefaultLOBTimeout() {
        return defaultLOBTimeout;
    }

    public String getDefaultLOBSuffix() {
        return defaultLOBSuffix;
    }

    public long getDefaultLOBVerificationBytes() {
        return defaultLOBVerificationBytes;
    }

    public Consistency getDefaultConsistency() {
        return defaultConsistency;
    }

    public Durability getDefaultDurability() {
        return defaultDurability;
    }

    public int getDefaultChunksPerPartition() {
        return defaultChunksPerPartition;
    }

    public int getDefaultChunkSize() {
        return defaultChunkSize;
    }

    @Override
    public Version putLOB(Key lobKey,
                          InputStream lobStream,
                          Durability durability,
                          long lobTimeout,
                          TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.putLOB(lobKey, lobStream,
                                      durability, lobTimeout, timeoutUnit);
    }

    @Override
    public boolean deleteLOB(Key lobKey,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit) {
        return largeObjectImpl.deleteLOB(lobKey, durability,
                                         lobTimeout, timeoutUnit);
    }

    @Override
    public InputStreamVersion getLOB(Key lobKey,
                                     Consistency consistency,
                                     long lobTimeout,
                                     TimeUnit timeoutUnit) {
        return largeObjectImpl.getLOB(lobKey, consistency,
                                      lobTimeout, timeoutUnit);
    }

    @Override
    public Version putLOBIfAbsent(Key lobKey,
                                  InputStream lobStream,
                                  Durability durability,
                                  long lobTimeout,
                                  TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            putLOBIfAbsent(lobKey, lobStream,
                           durability, lobTimeout, timeoutUnit);
    }

    @Override
    public Version putLOBIfPresent(Key lobKey,
                                   InputStream lobStream,
                                   Durability durability,
                                   long lobTimeout,
                                   TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            putLOBIfPresent(lobKey, lobStream,
                            durability, lobTimeout, timeoutUnit);
    }

    @Override
    public TableAPI getTableAPI() {
        return tableAPI;
    }

    public TableAPIImpl getTableAPIImpl() {
        return tableAPI;
    }

    @Override
    public Version appendLOB(Key lobKey,
                             InputStream lobAppendStream,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit)
        throws IOException {

        return largeObjectImpl.
            appendLOB(lobKey, lobAppendStream,
                      durability, lobTimeout, timeoutUnit);
    }

    /**
     * Attempt reauthentication, if possible.
     * @param requestLoginMgr the LoginManager in effect at the time of
     *   the request execution.
     * @return true if reauthentication has succeeded
     */
    public boolean tryReauthenticate(LoginManager requestLoginMgr)
        throws FaultException {

        if (reauthHandler == null) {
            return false;
        }

        synchronized (loginLock) {
            /*
             * If multiple threads are concurrently accessing the kvstore at
             * the time of an AuthenticationRequiredException, there is the
             * possibility of a flood of AuthenticationRequiredExceptions
             * occuring, with a flood of reauthentication attempts following.
             * Because of the synchronization on loginLock, only one thread
             * will be able to re-authenticate at a time, so by the time a
             * reauthentication request makes it here, another thread may
             * already have completed the authentication.
             */
            if (this.loginMgr == requestLoginMgr) {
                try {
                    if (isInternalHandle()) {
                        reauthHandler.reauthenticate(this.external);
                    } else {
                        reauthHandler.reauthenticate(this);
                    }
                } catch (KVSecurityException kvse) {
                    logger.fine(kvse.getMessage());
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Try to resolve the SSL handshake error in case it was caused by updated
     * SSL certificates, to allow an existing client to connect successfully
     * without reopening the store.
     */
    private void tryResolveSSLHandshakeError() {
        final String storeName =
            dispatcher.getTopologyManager().getTopology().getKVStoreName();

        /* Reread the truststore in case certificates have changed */
        RegistryUtils.resetRegistryCSF(storeName);

        /*
         * Reset all request handlers in an attempt to get around SSL handshake
         * errors encountered by the previous resolution. If the store has
         * updated SSL certificates, resetting the request handlers for all
         * RepNodes will cause new connections to be created that can take
         * advantage of new certificates.
         */
        final RepGroupStateTable repGroupStateTable =
            dispatcher.getRepGroupStateTable();
        for (RepNodeState state : repGroupStateTable.getRepNodeStates()) {
            state.resetReqHandlerRef();
        }
    }

    /**
     * Gets a task executor which will run at most maxConcurrentTasks tasks
     * using the shared thread pool. Once maxConcurrentTasks have been
     * submitted to the shared thread pool for execution further tasks are
     * queued for later execution.
     *
     * Tasks submitted through the executor will compete with all other tasks
     * in the shared thread pool.
     *
     * @param maxConcurrentTasks the maximum number of tasks that the
     * returned executor will submit to the shared thread pool for execution.
     *
     * @return a task executor
     */
    public TaskExecutor getTaskExecutor(int maxConcurrentTasks) {
        return sharedThreadPool.getTaskExecutor(maxConcurrentTasks);
    }

    /**
     * The task executor for the shared thread pool.
     */
    public interface TaskExecutor {

        /**
         * Submits a Runnable task for execution and returns a Future
         * representing that task. The future's get method will return null
         * upon successful completion.
         *
         * @param task the task to submit
         * @return a Future representing pending completion of the task
         * @throws RejectedExecutionException if the executor or the shared
         * thread pool has been shutdown
         */
        Future<?> submit(Runnable task);

        /**
         * Attempts to stop all actively executing tasks, halts the processing
         * of waiting tasks, and returns a list of the tasks that were awaiting
         * execution. These tasks are drained (removed) from the task queue
         * upon return from this method. Only the tasks submitted through this
         * executor are affected.
         *
         * Tasks are canceled via Thread.interrupt(), so any task that fails
         * to respond to interrupts may never terminate.
         *
         * @return a list of tasks that never commenced execution
         */
        List<Runnable> shutdownNow();
    }

    @Override
    public ExecutionFuture execute(String statement)
        throws IllegalArgumentException, FaultException {

        return execute(statement, new ExecuteOptions());
    }

    @Override
    public ExecutionFuture execute(String statement, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        checkClosed();

        if (options == null) {
            options = new ExecuteOptions();
        }

        PreparedStatement ps = prepare(statement, options);

        if (ps instanceof PreparedDdlStatementImpl) {
            /*
             * Use LogContext for calls from cloud's TenantManager.
             * Such calls will only be for DDL.
             */
            LogContext lc = (options == null ? null : options.getLogContext());

            return executeDdl((PreparedDdlStatementImpl) ps, lc);
        }

        return executeDml((PreparedStatementImpl) ps, options);
    }

    @Override
    public ExecutionFuture execute(char[] statement, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        checkClosed();

        if (options == null) {
            options = new ExecuteOptions();
        }

        PreparedStatement ps = prepare(statement, options);

        if (ps instanceof PreparedDdlStatementImpl) {
            /*
             * Use LogContext for calls from cloud's TenantManager.
             * Such calls will only be for DDL.
             */
            LogContext lc = (options == null ? null : options.getLogContext());
            return executeDdl((PreparedDdlStatementImpl) ps, lc);
        }

        return executeDml((PreparedStatementImpl) ps, options);
    }

    /* Check if the store handle has been closed */
    private void checkClosed() {
        if (isClosed) {
            throw new IllegalArgumentException(
                "Cannot execute request on a closed store handle");
        }
    }

    @Override
    public AsyncExecutionHandle executeAsync(String statement,
                                             ExecuteOptions options) {
        checkNull("statement", statement);
        checkClosed();
        return executeAsync(prepare(statement, options), options);
    }

    @Override
    public AsyncExecutionHandle executeAsync(Statement statement,
                                             ExecuteOptions options) {
        checkNull("statement", statement);
        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return ((InternalStatement) statement).executeAsync(this, options);
        } catch (AuthenticationRequiredException are) {
            if (!tryReauthenticate(requestLoginMgr)){
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to retry
             * the operation.  No retry on the authentication here.
             */
            return ((InternalStatement) statement).executeAsync(this, options);
        }
    }

    /**
     * Executes a DDL statement in an async fashion, returning an
     * ExecutionFuture.
     *
     * Because this method communicates directly with the admin it needs to
     * handle exceptions carefully.
     */
    private ExecutionFuture executeDdl(PreparedDdlStatementImpl statement,
                                       LogContext lc)
        throws IllegalArgumentException, FaultException {
        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return statementExecutor.executeDdl(statement.getQuery(),
                                                statement.getNamespace(),
                                                null, /* TableLimits */
                                                getLoginManager(this), null);
        } catch (AuthenticationRequiredException are) {
            if (!tryReauthenticate(requestLoginMgr)){
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return statementExecutor.executeDdl(statement.getQuery(),
                                                statement.getNamespace(),
                                                null, /* TableLimits */
                                                getLoginManager(this), lc);
        }
    }

    /**
     * Executes a DML statement in an async fashion, returning an
     * ExecutionFuture. In this path there is no async protocol at this time,
     * so the query is executed and the results are always immediately
     * available.
     */
    private ExecutionFuture executeDml(PreparedStatementImpl statement,
                                       ExecuteOptions options)
        throws IllegalArgumentException, FaultException {

        try {
            final StatementResult result = executeSync(statement, options);
            return new DmlFuture(result);
        } catch (QueryException qe) {

            /* A QueryException thrown at the client; rethrow as IAE */
            throw qe.getIllegalArgument();
        }
    }

    /**
     * Executes a set table limits in an async fashion, returning an
     * ExecutionFuture.
     *
     * This is public for access directly from the cloud proxy.
     */
    public ExecutionFuture setTableLimits(String namespace,
                                          String tableName,
                                          TableLimits limits)
        throws IllegalArgumentException, FaultException {

        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return statementExecutor.setTableLimits(namespace,
                                                    tableName,
                                                    limits,
                                                    getLoginManager(this));
        } catch (AuthenticationRequiredException are) {
            if (!tryReauthenticate(requestLoginMgr)){
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return statementExecutor.setTableLimits(namespace,
                                                    tableName,
                                                    limits,
                                                    getLoginManager(this));
        }
    }

    @Override
    public StatementResult executeSync(String statement)
        throws FaultException {

        return executeSync(statement, new ExecuteOptions());
    }

    @Override
    public StatementResult executeSync(String statement,
                                       ExecuteOptions options)
        throws FaultException, IllegalArgumentException {
        ExecutionFuture f = execute(statement, options);
        return DdlStatementExecutor.waitExecutionResult(f);
    }

    @Override
    public StatementResult executeSync(char[] statement,
                                       ExecuteOptions options)
        throws FaultException, IllegalArgumentException {
            ExecutionFuture f = execute(statement, options);
            return DdlStatementExecutor.waitExecutionResult(f);
    }

    @Override
    public ExecutionFuture getFuture(byte[] futureBytes) {

        return new DdlFuture(futureBytes, statementExecutor);
    }

    @Override
    public PreparedStatement prepare(String query)
        throws FaultException, IllegalArgumentException {

        return prepare(query, new ExecuteOptions());
    }

    @Override
    public PreparedStatement prepare(String query, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        try {
            return CompilerAPI.prepare(tableAPI, query.toCharArray(), options);
        } catch (QueryException qe) {
            /* rethrow as IAE */
            throw qe.getIllegalArgument();
        }
    }

    @Override
    public PreparedStatement prepare(char[] query, ExecuteOptions options)
        throws FaultException, IllegalArgumentException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        try {
            return CompilerAPI.prepare(tableAPI, query, options);
        } catch (QueryException qe) {
            /* rethrow as IAE */
            throw qe.getIllegalArgument();
        }
    }

    @Override
    public StatementResult executeSync(final Statement statement)
        throws FaultException {

        return executeSync(statement, new ExecuteOptions());
    }

    @Override
    public StatementResult executeSync(Statement statement,
                                       ExecuteOptions options)
        throws FaultException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final LoginManager requestLoginMgr = this.loginMgr;
        try {
            return ((InternalStatement)statement).executeSync(this, options);
        } catch (AuthenticationRequiredException are) {
            if (!tryReauthenticate(requestLoginMgr)){
                throw are;
            }

            /*
             * If the authentication completed, we assume we are ready to
             * retry the operation.  No retry on the authentication here.
             */
            return ((InternalStatement)statement).executeSync(this, options);
        }
    }

    /**
     * Not part of the public KVStore interface available to external clients.
     * This method is employed when the Oracle NoSQL DB Hive/BigDataSQL
     * integration mechanism is used to process a query and disjoint partition
     * sets are specified for each split.
     */
    public StatementResult executeSyncPartitions(final String statement,
                                                 ExecuteOptions options,
                                                 final Set<Integer> partitions)
        throws FaultException, IllegalArgumentException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final PreparedStatement ps = prepare(statement, options);

        if (!(ps instanceof PreparedStatementImpl)) {
            throw new IllegalArgumentException("unsupported statement type [" +
                                               ps.getClass().getName() + "]");
        }

        try {
            final StatementResult result =
                ((PreparedStatementImpl) ps).executeSyncPartitions(
                                                    this, options, partitions);
            return DdlStatementExecutor.waitExecutionResult(
                                            new DmlFuture(result));
        } catch (QueryException qe) {
            throw qe.getIllegalArgument(); /* Rethrow QueryException as IAE */
        }
    }

    /**
     * Not part of the public KVStore interface available to external clients.
     * This method is employed when the Oracle NoSQL DB Hive/BigDataSQL
     * integration mechanism is used to process a query and disjoint shard
     * sets are specified for each split.
     */
    public StatementResult executeSyncShards(final String statement,
                                             ExecuteOptions options,
                                             final Set<RepGroupId> shards)
        throws FaultException, IllegalArgumentException {

        if (options == null) {
            options = new ExecuteOptions();
        }

        final PreparedStatement ps = prepare(statement, options);

        if (!(ps instanceof PreparedStatementImpl)) {
            throw new IllegalArgumentException("unsupported statement type [" +
                                               ps.getClass().getName() + "]");
        }

        try {
            final StatementResult result =
                ((PreparedStatementImpl) ps).executeSyncShards(
                                                        this, options, shards);
            return DdlStatementExecutor.waitExecutionResult(
                                            new DmlFuture(result));
        } catch (QueryException qe) {
            throw qe.getIllegalArgument(); /* Rethrow QueryException as IAE */
        }
    }

    /**
     * Internal use (but public) interface to create a table using
     * read/write/storage limits
     */
    public ExecutionFuture execute(char[] statement,
                                   ExecuteOptions options,
                                   TableLimits limits)
        throws FaultException, IllegalArgumentException {

        checkClosed();

        if (options == null) {
            options = new ExecuteOptions();
        }

        PreparedStatement ps = prepare(statement, options);

        if (ps instanceof PreparedDdlStatementImpl) {
            String namespace = null;
            LogContext lc = null;
            if (options != null) {
                namespace = options.getNamespace();
                lc = options.getLogContext();
            }
            return statementExecutor.executeDdl(statement,
                                                namespace,
                                                limits,
                                                getLoginManager(this), lc);
        }
        throw new IllegalArgumentException(
            "Execute with TableLimits is restricted to DDL operations");
    }

    /**
     * Utility method to create a KeyValueVersion. If expiration time is
     * non-zero it creates KeyValueVersionInternal to hold it. This allows
     * space optimization for the more common case where there is no
     * expiration time.
     */
    public static KeyValueVersion createKeyValueVersion(
        final Key key,
        final Value value,
        final Version version,
        final long expirationTime) {

        if (expirationTime == 0) {
            return new KeyValueVersion(key, value, version);
        }
        return new KeyValueVersionInternal(key, value, version, expirationTime);
    }

    public DdlStatementExecutor getDdlStatementExecutor() {
        return statementExecutor;
    }
}
