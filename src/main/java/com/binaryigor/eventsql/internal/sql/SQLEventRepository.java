package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.internal.EventInput;
import com.binaryigor.eventsql.internal.EventRepository;
import com.binaryigor.eventsql.internal.Transactions;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLEventRepository implements EventRepository {

    private static final Logger logger = LoggerFactory.getLogger(SQLEventRepository.class);
    private static final Table<?> EVENT_BUFFER = DSL.table("event_buffer");
    private static final Table<?> EVENT_BUFFER_LOCK = DSL.table("event_buffer_lock");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<Long> ID = DSL.field("id", Long.class);
    private static final Field<String> EVENT_BUFFER_LOCK_ID = DSL.field("id", String.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<String> KEY = DSL.field("key", String.class);
    private static final Field<byte[]> VALUE = DSL.field("value", byte[].class);
    private static final Field<JSON> METADATA = DSL.field("metadata", JSON.class);
    private static final Field<Timestamp> BUFFERED_AT = DSL.field("buffered_at", Timestamp.class);
    private static final Field<Timestamp> CREATED_AT = DSL.field("created_at", Timestamp.class);
    private final Transactions transactions;
    private final DSLContextProvider contextProvider;

    public SQLEventRepository(Transactions transactions, DSLContextProvider contextProvider) {
        this.transactions = transactions;
        this.contextProvider = contextProvider;
    }

    @Override
    public void createBuffer() {
        contextProvider.get()
                .createTableIfNotExists(EVENT_BUFFER)
                .column(ID, ID.getDataType().identity(true))
                .column(TOPIC, TOPIC.getDataType().notNull())
                .column(PARTITION, PARTITION.getDataType().notNull())
                .column(KEY)
                .column(VALUE, VALUE.getDataType().notNull())
                .column(CREATED_AT, CREATED_AT.getDataType().notNull().defaultValue(DSL.now()))
                .column(METADATA, METADATA.getDataType().notNull())
                .constraint(DSL.constraint().primaryKey(ID))
                .execute();
    }

    @Override
    public void prepareBufferLock() {
        transactions.execute(() -> {
            var tContext = contextProvider.get();

            tContext.createTableIfNotExists(EVENT_BUFFER_LOCK)
                    .column(EVENT_BUFFER_LOCK_ID)
                    .constraint(DSL.primaryKey(EVENT_BUFFER_LOCK_ID))
                    .execute();

            var locksCount = tContext.fetchCount(EVENT_BUFFER_LOCK);
            if (locksCount == 1) {
                logger.info("There is exactly one {} record, not need to create it", EVENT_BUFFER_LOCK);
                return;
            }
            if (locksCount > 1) {
                logger.warn("For some reason, there is more than on {} record, removing them", EVENT_BUFFER_LOCK);
                tContext.deleteFrom(EVENT_BUFFER_LOCK).execute();
            }

            logger.info("Creating singleton {} record", EVENT_BUFFER_LOCK);
            tContext.insertInto(EVENT_BUFFER_LOCK)
                    .columns(EVENT_BUFFER_LOCK_ID)
                    .values("singleton-lock")
                    .execute();
        });
    }

    @Override
    public void createPartition(String topic) {
        contextProvider.get()
                .createTableIfNotExists(eventTable(topic))
                .column(ID, ID.getDataType().identity(true))
                .column(PARTITION, PARTITION.getDataType().notNull())
                .column(KEY)
                .column(VALUE, VALUE.getDataType().notNull())
                .column(BUFFERED_AT, BUFFERED_AT.getDataType().notNull())
                .column(CREATED_AT, CREATED_AT.getDataType().notNull().defaultValue(DSL.now()))
                .column(METADATA, METADATA.getDataType().notNull())
                .constraint(DSL.constraint().primaryKey(ID))
                .execute();
    }

    private Table<?> eventTable(String topic) {
        return DSL.table(topic + "_event");
    }

    @Override
    public void dropPartition(String topic) {
        contextProvider.get()
                .dropTableIfExists(eventTable(topic))
                .execute();
    }

    @Override
    public void create(EventInput event) {
        createAll(List.of(event));
    }

    @Override
    public void createAll(Collection<EventInput> events) {
        if (events.isEmpty()) {
            return;
        }

        var insert = contextProvider.get()
                .insertInto(EVENT_BUFFER)
                .columns(TOPIC, PARTITION, KEY, VALUE, METADATA);

        events.forEach(ei -> {
            var e = ei.publication();
            var metadataJSON = SimpleJSONMapper.toJSON(e.metadata());
            insert.values(e.topic(), ei.partition(), e.key(), e.value(), JSON.valueOf(metadataJSON));
        });

        insert.execute();
    }

    // TODO: maybe per topic?
    @Override
    public int flushBuffer(int toFlush) {
        var toFlushOverLimit = toFlush + 1;
        var flushed = new AtomicInteger(0);

        transactions.execute(() -> {
            var tContext = contextProvider.get();
            var lock = tContext.select(EVENT_BUFFER_LOCK_ID)
                    .from(EVENT_BUFFER_LOCK)
                    .forUpdate()
                    .skipLocked()
                    .fetch();
            if (lock.isEmpty()) {
                return;
            }
            if (lock.size() > 1) {
                throw new IllegalArgumentException("%s table has %d non-locked records but should have just one; fix your db state!"
                        .formatted(EVENT_BUFFER_LOCK, lock.size()));
            }

            var eventBufferIdsByTopic = tContext.select(TOPIC, ID)
                    .from(EVENT_BUFFER)
                    .orderBy(ID)
                    .limit(toFlushOverLimit)
                    .fetchGroups(TOPIC, ID);
            if (eventBufferIdsByTopic.isEmpty()) {
                return;
            }

            var queriesToBatch = new ArrayList<Query>();

            eventBufferIdsByTopic.forEach((topic, ids) -> {
                queriesToBatch.add(tContext.insertInto(eventTable(topic))
                        .columns(PARTITION, KEY, VALUE, METADATA, BUFFERED_AT)
                        .select(tContext.select(PARTITION, KEY, VALUE, METADATA, CREATED_AT.as(BUFFERED_AT))
                                .from(EVENT_BUFFER)
                                .where(ID.in(ids))));

                flushed.addAndGet(ids.size());
            });

            var insertedIds = eventBufferIdsByTopic.values().stream().flatMap(List::stream).toList();
            queriesToBatch.add(tContext.deleteFrom(EVENT_BUFFER)
                    .where(ID.in(insertedIds)));

            tContext.batch(queriesToBatch).execute();
        });

        return flushed.get();
    }

    @Override
    public List<Event> nextEvents(String topic, Long lastId, int limit) {
        return nextEvents(topic, null, lastId, limit);
    }

    public List<Event> nextEvents(String topic, Short partition, Long lastId, int limit) {
        Condition condition = DSL.trueCondition();
        if (lastId != null) {
            condition = condition.and(ID.greaterThan(lastId));
        }
        if (partition != null) {
            condition = condition.and(PARTITION.eq(partition));
        }
        return contextProvider.get()
                .select(DSL.val(topic).as(TOPIC), ID, PARTITION, KEY, VALUE, METADATA)
                .from(eventTable(topic))
                .where(condition)
                .orderBy(ID)
                .limit(limit)
                .fetchInto(Event.class);
    }

    @Override
    public List<Event> nextEvents(String topic, int partition, Long lastId, int limit) {
        return nextEvents(topic, Short.valueOf((short) partition), lastId, limit);
    }
}
