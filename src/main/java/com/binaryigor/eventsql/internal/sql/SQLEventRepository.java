package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.internal.EventInput;
import com.binaryigor.eventsql.internal.EventRepository;
import com.binaryigor.eventsql.internal.Transactions;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLEventRepository implements EventRepository {

    private static final Table<?> EVENT_BUFFER = DSL.table("event_buffer");
    private static final Table<?> EVENT_BUFFER_LOCK = DSL.table("event_buffer_lock");
    private static final Field<String> TOPIC = DSL.field("eql_topic", String.class);
    private static final Field<Long> ID = DSL.field("eql_id", Long.class);
    private static final Field<Short> PARTITION = DSL.field("eql_partition", Short.class);
    private static final Field<String> KEY = DSL.field("eql_key", String.class);
    private static final Field<byte[]> VALUE = DSL.field("eql_value", byte[].class);
    private static final Field<JSON> METADATA = DSL.field("eql_metadata", JSON.class);
    private static final Field<Timestamp> BUFFERED_AT = DSL.field("eql_buffered_at", Timestamp.class);
    private static final Field<Timestamp> CREATED_AT = DSL.field("eql_created_at", Timestamp.class);
    private final Transactions transactions;
    private final DSLContextProvider contextProvider;

    public SQLEventRepository(Transactions transactions, DSLContextProvider contextProvider) {
        this.transactions = transactions;
        this.contextProvider = contextProvider;
    }

    @Override
    public void createBuffer() {
        transactions.execute(() -> {
            contextProvider.get()
                    .createTableIfNotExists(EVENT_BUFFER)
                    .column(ID, ID.getDataType().identity(true))
                    // MYSQL requires to limit text length for every indexed column
                    .column(TOPIC, TOPIC.getDataType().notNull().length(255))
                    .column(PARTITION, PARTITION.getDataType().notNull())
                    .column(KEY)
                    .column(VALUE, VALUE.getDataType().notNull())
                    .column(CREATED_AT, CREATED_AT.getDataType().notNull().defaultValue(DSL.now()))
                    .column(METADATA, METADATA.getDataType().notNull())
                    .constraint(DSL.primaryKey(ID))
                    .execute();

            try {
                // MySQL does not provide proper create index if not exists semantics
                contextProvider.get()
                        .createIndex("event_buffer_topic_id")
                        .on(EVENT_BUFFER, TOPIC, ID)
                        .execute();
            } catch (DataAccessException e) {
                var loweredMessage  = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
                if (!loweredMessage.contains("duplicate") && !loweredMessage.contains("exists")) {
                    throw e;
                }
            }
        });
    }

    @Override
    public void createPartition(String topic) {
        transactions.execute(() -> {
            var tContext = contextProvider.get();

            tContext.createTableIfNotExists(eventTable(topic))
                    .column(ID, ID.getDataType().identity(true))
                    .column(PARTITION, PARTITION.getDataType().notNull())
                    .column(KEY)
                    .column(VALUE, VALUE.getDataType().notNull())
                    .column(BUFFERED_AT, BUFFERED_AT.getDataType().notNull())
                    .column(CREATED_AT, CREATED_AT.getDataType().notNull().defaultValue(DSL.now()))
                    .column(METADATA, METADATA.getDataType().notNull())
                    .constraint(DSL.constraint().primaryKey(ID))
                    .execute();

            tContext.createTableIfNotExists(EVENT_BUFFER_LOCK)
                    .column(TOPIC, TOPIC.getDataType().length(255))
                    .constraint(DSL.primaryKey(TOPIC))
                    .execute();

            tContext.insertInto(EVENT_BUFFER_LOCK)
                    .columns(TOPIC)
                    .values(topic)
                    .onConflictDoNothing()
                    .execute();
        });
    }

    private Table<?> eventTable(String topic) {
        return DSL.table(topic + "_event");
    }

    // TODO: lacking test cases
    @Override
    public void dropPartition(String topic) {
        transactions.execute(() -> {
            var tContext = contextProvider.get();

            tContext.dropTableIfExists(eventTable(topic))
                    .execute();

            tContext.deleteFrom(EVENT_BUFFER)
                    .where(TOPIC.eq(topic))
                    .execute();
            tContext.deleteFrom(EVENT_BUFFER_LOCK)
                    .where(TOPIC.eq(topic))
                    .execute();
        });
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

    // TODO: lacking test cases
    @Override
    public int flushBuffer(Collection<String> topics, int toFlush) {
        if (topics.isEmpty()) {
            return 0;
        }

        var toFlushOverLimit = toFlush + 1;
        var flushed = new AtomicInteger(0);

        transactions.execute(() -> {
            var tContext = contextProvider.get();
            var lockedTopics = tContext.select(TOPIC)
                    .from(EVENT_BUFFER_LOCK)
                    .where(TOPIC.in(topics))
                    .forUpdate()
                    .skipLocked()
                    .fetch(TOPIC);
            if (lockedTopics.isEmpty()) {
                return;
            }

            var eventBufferIdsByTopic = tContext.select(TOPIC, ID)
                    .from(EVENT_BUFFER)
                    .where(TOPIC.in(lockedTopics))
                    .orderBy(TOPIC, ID)
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
