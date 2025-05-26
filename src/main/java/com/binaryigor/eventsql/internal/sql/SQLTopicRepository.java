package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.TopicDefinition;
import com.binaryigor.eventsql.internal.TopicRepository;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

public class SQLTopicRepository implements TopicRepository {

    private static final Table<?> TOPIC = DSL.table("topic");
    private static final Field<String> NAME = DSL.field("name", String.class);
    private static final Field<Short> PARTITIONS = DSL.field("partitions", Short.class);
    private static final Field<Timestamp> CREATED_AT = DSL.field("created_at", Timestamp.class);
    private final DSLContextProvider contextProvider;

    public SQLTopicRepository(DSLContextProvider contextProvider) {
        this.contextProvider = contextProvider;
    }

    @Override
    public void createTable() {
        contextProvider.get()
                .createTableIfNotExists(TOPIC)
                .column(NAME)
                .column(PARTITIONS, PARTITIONS.getDataType().notNull())
                .column(CREATED_AT, CREATED_AT.getDataType().notNull().defaultValue(DSL.now()))
                .constraint(DSL.constraint().primaryKey(NAME))
                .execute();
    }

    @Override
    public void save(TopicDefinition topic) {
        contextProvider.get()
                .insertInto(TOPIC)
                .columns(NAME, PARTITIONS)
                .values(topic.name(), (short) topic.partitions())
                .onConflict(NAME)
                .doUpdate()
                .set(PARTITIONS, (short) topic.partitions())
                .execute();
    }

    @Override
    public Optional<TopicDefinition> ofName(String name) {
        return contextProvider.get()
                .select(NAME, PARTITIONS)
                .from(TOPIC)
                .where(NAME.eq(name))
                .fetchOptionalInto(TopicDefinition.class);
    }

    @Override
    public List<TopicDefinition> all() {
        return contextProvider.get()
                .select(NAME, PARTITIONS)
                .from(TOPIC)
                .fetchInto(TopicDefinition.class);
    }

    @Override
    public void delete(String topic) {
        contextProvider.get()
                .deleteFrom(TOPIC)
                .where(NAME.eq(topic))
                .execute();
    }
}
