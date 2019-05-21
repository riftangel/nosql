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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.AdminStores.AdminStore;
import oracle.kv.impl.admin.AdminStores.AdminStoreCursor;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.criticalevent.CriticalEvent.EventKey;
import oracle.kv.impl.admin.criticalevent.CriticalEvent.EventType;
import oracle.kv.impl.util.SerializationUtil;

/*
 * Store for critical events.
 */
public abstract class EventStore extends AdminStore {

    public static EventStore getReadOnlyInstance(Logger logger,
                                                 Environment env) {
        return new EventDatabaseStore(logger, env, true);
    }

    static EventStore getStoreByVersion(int schemaVersion,
                                        Admin admin, EntityStore eStore) {
        if (schemaVersion < AdminSchemaVersion.SCHEMA_VERSION_5) {
            assert eStore != null;
            return new EventDPLStore(admin.getLogger(), eStore);
        }
        return new EventDatabaseStore(admin.getLogger(), admin.getEnv(),
                                      false /* read only */);
    }

    protected EventStore(Logger logger) {
        super(logger);
    }

    public abstract EventCursor getEventCursor();

    /**
     * Retrieve a single event from the database, using the given key.
     */
    CriticalEvent getEvent(Transaction txn, String eventId) {
        return getEvent(txn, EventKey.fromString(eventId));
    }

    protected abstract CriticalEvent getEvent(Transaction txn, EventKey key);

    /**
     * Retrieve a list of events matching the given criteria.
     */
    List<CriticalEvent> getEvents(Transaction txn,
                                  long startTime, long endTime,
                                  EventType type) {
        final EventKey startKey = new EventKey(startTime, type);
        final EventKey endKey =
                    new EventKey(endTime == 0 ? Long.MAX_VALUE : endTime, type);
        return getEvents(txn, startKey, endKey, type);
    }

    protected abstract List<CriticalEvent> getEvents(Transaction txn,
                                                     EventKey startKey,
                                                     EventKey endKey,
                                                     EventType type);

    abstract void putEvent(Transaction txn, CriticalEvent event);

    /**
     * Expire older events from the persistent store.
     */
    void ageStore(Transaction txn, long pruningAge) {
        final long expiry = new Date().getTime() - pruningAge;
        final EventKey startKey = new EventKey(0L, EventType.ALL);
        final EventKey endKey = new EventKey(expiry, EventType.ALL);
        ageStore(txn, startKey, endKey);
    }

    protected abstract void ageStore(Transaction txn,
                                     EventKey startKey,
                                     EventKey endKey);

    /*
     * JE Database subclass.
     */
    static class EventDatabaseStore extends EventStore {
        private final AdminDatabase<EventKey, CriticalEvent> eventDb;

        private EventDatabaseStore(Logger logger, Environment env,
                                   boolean readOnly) {
            super(logger);
            eventDb = new AdminDatabase<EventKey, CriticalEvent>(DB_TYPE.EVENTS,
                                                                 logger, env,
                                                                 readOnly) {
                @Override
                protected DatabaseEntry keyToEntry(EventKey key) {
                    /*
                     * The key generated is the same as the original DPL
                     * key. This was done to retain the sort order.
                     */
                    final DatabaseEntry keyEntry = new DatabaseEntry();
                    final TupleOutput to = new TupleOutput();
                    to.writeLong(key.getSyntheticTimestamp());
                    to.writeByte(0);
                    to.writeChars(key.getCategory());
                    TupleBinding.outputToEntry(to, keyEntry);
                    return keyEntry;
                }};
        }

        @Override
        public EventCursor getEventCursor() {
            return new EventCursor(eventDb.openCursor(null)) {

                @Override
                protected CriticalEvent entryToObject(DatabaseEntry key,
                                                      DatabaseEntry value) {
                    return SerializationUtil.getObject(value.getData(),
                                                       CriticalEvent.class);
                }
            };
        }

        @Override
        protected CriticalEvent getEvent(Transaction txn, EventKey key) {
            return eventDb.get(txn, key,
                               LockMode.READ_COMMITTED, CriticalEvent.class);
        }

        @Override
        protected List<CriticalEvent> getEvents(Transaction txn,
                                                EventKey startKey,
                                                EventKey endKey,
                                                EventType type) {
            final List<CriticalEvent> events = new ArrayList<>();
            final DatabaseEntry keyEntry = eventDb.keyToEntry(startKey);
            final DatabaseEntry endEntry = eventDb.keyToEntry(endKey);
            final DatabaseEntry valueEntry = new DatabaseEntry();

            try (final Cursor cursor = eventDb.openCursor(txn)) {
                OperationStatus status =
                                cursor.getSearchKeyRange(keyEntry, valueEntry,
                                                         LockMode.DEFAULT);
                while (status == OperationStatus.SUCCESS) {
                    if (eventDb.compareKeys(keyEntry, endEntry) > 0) {
                        break;
                    }
                    final CriticalEvent event =
                               SerializationUtil.getObject(valueEntry.getData(),
                                                           CriticalEvent.class);
                    if (type == EventType.ALL ||
                        type.equals(event.getEventType())) {
                        events.add(event);
                    }
                    status = cursor.getNext(keyEntry, valueEntry,
                                            LockMode.DEFAULT);
                }
            }
            return events;
        }

        @Override
        void putEvent(Transaction txn, CriticalEvent event) {
            eventDb.put(txn, event.getKey(), event, false);
        }

        @Override
        protected void ageStore(Transaction txn,
                                EventKey startKey,
                                EventKey endKey) {
            final DatabaseEntry keyEntry = eventDb.keyToEntry(startKey);
            final DatabaseEntry endEntry = eventDb.keyToEntry(endKey);
            final DatabaseEntry valueEntry = new DatabaseEntry();
            try (final Cursor cursor = eventDb.openCursor(txn)) {
                OperationStatus status =
                                cursor.getSearchKeyRange(keyEntry, valueEntry,
                                                         LockMode.DEFAULT);
                while (status == OperationStatus.SUCCESS) {
                    if (eventDb.compareKeys(keyEntry, endEntry) > 0) {
                        return;
                    }
                    cursor.delete();
                    status = cursor.getNext(keyEntry, valueEntry,
                                            LockMode.DEFAULT);
                }
            }
        }

        @Override
        public void close() {
            eventDb.close();
        }
    }

    /*
     * DPL subclass.
     */
    static class EventDPLStore extends EventStore {
        private final EntityStore eStore;

        private EventDPLStore(Logger logger, EntityStore eStore) {
            super(logger);
            this.eStore = eStore;
        }

        @Override
        public EventCursor getEventCursor() {
            /*
             * The cursor is only used in the dump utility. Since the utility
             * does not support DPL, this implementation is not needed.
             */
            throw new IllegalStateException("Operation is not supported");
        }

        @Override
        protected CriticalEvent getEvent(Transaction txn, EventKey key) {

            final PrimaryIndex<EventKey, CriticalEvent> pi =
                    eStore.getPrimaryIndex(EventKey.class, CriticalEvent.class);
            return pi.get(txn, key, LockMode.READ_UNCOMMITTED);
        }

        @Override
        protected List<CriticalEvent> getEvents(Transaction txn,
                                                EventKey startKey,
                                                EventKey endKey,
                                                EventType type) {
            final PrimaryIndex<EventKey, CriticalEvent> pi =
                    eStore.getPrimaryIndex(EventKey.class, CriticalEvent.class);
            final List<CriticalEvent> events = new ArrayList<>();

            try (final EntityCursor<CriticalEvent> eventCursor =
                                 pi.entities(txn, startKey, true, endKey, false,
                                             CursorConfig.READ_UNCOMMITTED)) {
                for (CriticalEvent ev : eventCursor) {
                    if (type == EventType.ALL ||
                        type.equals(ev.getEventType())) {
                        events.add(ev);
                    }
                }
            }
            return events;
        }

        @Override
        void putEvent(Transaction txn, CriticalEvent event) {
            readOnly();
        }

        @Override
        protected void ageStore(Transaction txn,
                                EventKey startKey,
                                EventKey endKey) {
            readOnly();
        }

        /**
         * Transfers all events stored in DPL store to non-DPL JE database.
         */
        @Override
        protected void convertTo(int existingVersion, AdminStore newStore,
                                 Transaction txn) {
            final EventStore newEventStore = (EventStore)newStore;
            final PrimaryIndex<EventKey, CriticalEvent> pi =
                    eStore.getPrimaryIndex(EventKey.class, CriticalEvent.class);

            try (final EntityCursor<CriticalEvent> eventCursor =
                              pi.entities(txn, CursorConfig.READ_UNCOMMITTED)) {
                for (CriticalEvent ev : eventCursor) {
                    newEventStore.putEvent(txn, ev);
                }
            }
        }
    }

    /**
     * A cursor class to facilitate the scan of the event store.
     */
    public abstract static class EventCursor
                extends AdminStoreCursor<EventKey, CriticalEvent> {

        private EventCursor(Cursor cursor) {
            super(cursor);
        }
     }
}
