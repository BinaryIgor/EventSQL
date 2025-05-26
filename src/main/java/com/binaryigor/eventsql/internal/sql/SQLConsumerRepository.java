package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.internal.Consumer;
import com.binaryigor.eventsql.internal.ConsumerId;
import com.binaryigor.eventsql.internal.ConsumerRepository;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class SQLConsumerRepository implements ConsumerRepository {

    private static final Table<?> CONSUMER = DSL.table("consumer");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<String> NAME = DSL.field("name", String.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<Long> FIRST_EVENT_ID = DSL.field("first_event_id", Long.class);
    private static final Field<Long> LAST_EVENT_ID = DSL.field("last_event_id", Long.class);
    private static final Field<Instant> LAST_CONSUMPTION_AT = DSL.field("last_consumption_at", Instant.class);
    private static final Field<Long> CONSUMED_EVENTS = DSL.field("consumed_events", Long.class);
    private static final Field<Timestamp> CREATED_AT = DSL.field("created_at", Timestamp.class);
    private final DSLContextProvider contextProvider;

    public SQLConsumerRepository(DSLContextProvider contextProvider) {
        this.contextProvider = contextProvider;
    }

    @Override
    public void createTable() {
        contextProvider.get()
                .createTableIfNotExists(CONSUMER)
                .column(TOPIC, TOPIC.getDataType().notNull())
                .column(NAME, NAME.getDataType().notNull())
                .column(PARTITION, PARTITION.getDataType().notNull())
                .column(FIRST_EVENT_ID)
                .column(LAST_EVENT_ID)
                .column(LAST_CONSUMPTION_AT, SQLDataType.TIMESTAMP)
                .column(CONSUMED_EVENTS, CONSUMED_EVENTS.getDataType().notNull())
                .column(CREATED_AT, CREATED_AT.getDataType().notNull().defaultValue(DSL.now()))
                .constraint(DSL.constraint().primaryKey(TOPIC, NAME, PARTITION))
                .execute();
    }

    @Override
    public void save(Consumer consumer) {
        saveAll(List.of(consumer));
    }

    @Override
    public void saveAll(Collection<Consumer> consumers) {
        if (consumers.isEmpty()) {
            return;
        }
        var insert = contextProvider.get()
                .insertInto(CONSUMER)
                .columns(TOPIC, NAME, PARTITION, FIRST_EVENT_ID, LAST_EVENT_ID, LAST_CONSUMPTION_AT, CONSUMED_EVENTS);

        consumers.forEach(c -> insert.values(c.topic(), c.name(), (short) c.partition(),
                c.firstEventId(), c.lastEventId(), c.lastConsumptionAt(), c.consumedEvents()));

        insert.onConflict(TOPIC, NAME, PARTITION)
                .doUpdate()
                .set(FIRST_EVENT_ID, DSL.excluded(FIRST_EVENT_ID))
                .set(LAST_EVENT_ID, DSL.excluded(LAST_EVENT_ID))
                .set(LAST_CONSUMPTION_AT, DSL.excluded(LAST_CONSUMPTION_AT))
                .set(CONSUMED_EVENTS, DSL.excluded(CONSUMED_EVENTS))
                .execute();
    }

    @Override
    public List<Consumer> all() {
        return allOf(DSL.trueCondition());
    }

    private List<Consumer> allOf(Condition condition) {
        return contextProvider.get()
                .select(TOPIC, NAME, PARTITION, FIRST_EVENT_ID, LAST_EVENT_ID, LAST_CONSUMPTION_AT, CONSUMED_EVENTS)
                .from(CONSUMER)
                .where(condition)
                .fetchInto(Consumer.class);
    }

    @Override
    public List<Consumer> allOf(String topic, String name) {
        return allOf(TOPIC.eq(topic).and(NAME.eq(name)));
    }

    @Override
    public List<Consumer> allOf(String topic) {
        return allOf(TOPIC.eq(topic));
    }

    @Override
    public Optional<Consumer> ofIdForUpdateSkippingLocked(ConsumerId id) {
        return contextProvider.get()
                .select(TOPIC, NAME, PARTITION, FIRST_EVENT_ID, LAST_EVENT_ID, LAST_CONSUMPTION_AT, CONSUMED_EVENTS)
                .from(CONSUMER)
                .where(TOPIC.eq(id.topic())
                        .and(NAME.eq(id.name()))
                        .and(PARTITION.eq((short) id.partition())))
                .forUpdate()
                .skipLocked()
                .fetchOptionalInto(Consumer.class);
    }

    @Override
    public void update(Consumer consumer) {
        contextProvider.get()
                .update(CONSUMER)
                .set(FIRST_EVENT_ID, consumer.firstEventId())
                .set(LAST_EVENT_ID, consumer.lastEventId())
                .set(LAST_CONSUMPTION_AT, consumer.lastConsumptionAt())
                .set(CONSUMED_EVENTS, consumer.consumedEvents())
                .where(TOPIC.eq(consumer.topic())
                        .and(NAME.eq(consumer.name()))
                        .and(PARTITION.eq((short) consumer.partition())))
                .execute();
    }

    @Override
    public void deleteAllOf(String topic, String name) {
        contextProvider.get()
                .delete(CONSUMER)
                .where(TOPIC.eq(topic).and(NAME.eq(name)))
                .execute();
    }
}
