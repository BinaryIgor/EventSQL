package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.EventSQLDialect;
import com.binaryigor.eventsql.internal.EventInput;
import com.binaryigor.eventsql.internal.EventRepository;
import com.binaryigor.eventsql.internal.Transactions;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLEventRepository implements EventRepository {

    private static final Logger logger = LoggerFactory.getLogger(SQLEventRepository.class);
    private static final Table<?> EVENT = DSL.table("event");
    private static final Table<?> EVENT_BUFFER = DSL.table("event_buffer");
    private static final Table<?> EVENT_BUFFER_LOCK = DSL.table("event_buffer_lock");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<Long> ID = DSL.field("id", Long.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<String> KEY = DSL.field("key", String.class);
    private static final Field<byte[]> VALUE = DSL.field("value", byte[].class);
    private static final Field<JSON> METADATA = DSL.field("metadata", JSON.class);
    private static final Field<Timestamp> BUFFERED_AT = DSL.field("buffered_at", Timestamp.class);
    private static final Field<Timestamp> CREATED_AT = DSL.field("created_at", Timestamp.class);
    private final Transactions transactions;
    private final DSLContextProvider contextProvider;
    private final EventSQLDialect dialect;

    public SQLEventRepository(Transactions transactions, DSLContextProvider contextProvider, EventSQLDialect dialect) {
        this.transactions = transactions;
        this.contextProvider = contextProvider;
        this.dialect = dialect;
    }

    @Override
    public void createPartition(String topic) {
        if (dialect == EventSQLDialect.POSTGRES) {
            contextProvider.get()
                    .execute("CREATE TABLE %s PARTITION OF %s FOR VALUES IN ('%s')"
                            .formatted(topicTablePartitionName(topic), EVENT.getName(), topic));
        } else {
            warnPartitionManagementNotSupportedForDialect();
        }
    }

    private void warnPartitionManagementNotSupportedForDialect() {
        logger.warn("Partition management is not supported for {} SQL dialect. Manage it manually or consider opening a Pull Request :)", dialect);
    }

    private String topicTablePartitionName(String topic) {
        return "%s_%s".formatted(topic, EVENT.getName());
    }

    @Override
    public void deletePartition(String topic) {
        if (dialect == EventSQLDialect.POSTGRES) {
            contextProvider.get()
                    .dropTableIfExists(topicTablePartitionName(topic))
                    .cascade()
                    .execute();
        } else {
            warnPartitionManagementNotSupportedForDialect();
        }
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

    @Override
    public int flushBuffer(int toFlush) {
        var toFlushOverLimit = toFlush + 1;
        var flushed = new AtomicInteger(0);

        transactions.execute(() -> {
            var tContext = contextProvider.get();
            var lock = tContext.select(CREATED_AT)
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

            var eventBufferIds = tContext.select(ID)
                    .from(EVENT_BUFFER)
                    .orderBy(ID)
                    .limit(toFlushOverLimit)
                    .fetch(ID);
            if (eventBufferIds.isEmpty()) {
                return;
            }

            tContext.insertInto(EVENT)
                    .columns(TOPIC, PARTITION, KEY, VALUE, METADATA, BUFFERED_AT)
                    .select(tContext.select(TOPIC, PARTITION, KEY, VALUE, METADATA, CREATED_AT.as(BUFFERED_AT))
                            .from(EVENT_BUFFER)
                            .where(ID.in(eventBufferIds)))
                    .execute();

            tContext.deleteFrom(EVENT_BUFFER)
                    .where(ID.in(eventBufferIds))
                    .execute();

            flushed.set(toFlushOverLimit);
        });

        return flushed.get();
    }

    @Override
    public List<Event> nextEvents(String topic, Long lastId, int limit) {
        return nextEvents(topic, null, lastId, limit);
    }

    public List<Event> nextEvents(String topic, Short partition, Long lastId, int limit) {
        var condition = TOPIC.eq(topic);
        if (lastId != null) {
            condition = condition.and(ID.greaterThan(lastId));
        }
        if (partition != null) {
            condition = condition.and(PARTITION.eq(partition));
        }
        return contextProvider.get()
                .select(TOPIC, ID, PARTITION, KEY, VALUE, METADATA)
                .from(EVENT)
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
