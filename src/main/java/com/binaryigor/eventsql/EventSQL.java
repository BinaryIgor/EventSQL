package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.DefaultEventSQLConsumers;
import com.binaryigor.eventsql.internal.DefaultEventSQLPublisher;
import com.binaryigor.eventsql.internal.DefaultEventSQLRegistry;
import com.binaryigor.eventsql.internal.TopicDefinitionsCache;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLConsumers;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLPublisher;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLRegistry;
import com.binaryigor.eventsql.internal.sql.SQLConsumerRepository;
import com.binaryigor.eventsql.internal.sql.SQLEventRepository;
import com.binaryigor.eventsql.internal.sql.SQLTopicRepository;
import com.binaryigor.eventsql.internal.sql.SQLTransactions;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class EventSQL {

    public static final int DEFAULT_FLUSH_PUBLISH_BUFFER_SIZE = 1000;
    // delay is applied only if there is no more records to flush; see DefaultEventSQLPublisher.startFlushPublishBufferThread()
    public static final Duration DEFAULT_FLUSH_PUBLISH_BUFFER_DELAY = Duration.ofMillis(250);
    private final EventSQLRegistry registry;
    private final EventSQLPublisher publisher;
    private final EventSQLConsumers consumers;

    public EventSQL(DataSource dataSource, EventSQLDialect sqlDialect) {
        this(dataSource, sqlDialect, Clock.systemUTC());
    }

    public EventSQL(DataSource dataSource, EventSQLDialect sqlDialect, Clock clock) {
        this(List.of(dataSource), sqlDialect, clock);
    }

    public EventSQL(Collection<DataSource> dataSources, EventSQLDialect sqlDialect) {
        this(dataSources, sqlDialect, Clock.systemUTC());
    }

    public EventSQL(Collection<DataSource> dataSources, EventSQLDialect sqlDialect, Clock clock) {
        this(dataSources, sqlDialect, clock, DEFAULT_FLUSH_PUBLISH_BUFFER_SIZE, DEFAULT_FLUSH_PUBLISH_BUFFER_DELAY);
    }

    public EventSQL(Collection<DataSource> dataSources, EventSQLDialect sqlDialect, Clock clock,
                    int flushPublishBufferSize, Duration flushPublishBufferDelay) {
        if (dataSources.isEmpty()) {
            throw new IllegalArgumentException("At least one data source is required");
        }

        var registryList = new ArrayList<EventSQLRegistry>();
        var publisherList = new ArrayList<EventSQLPublisher>();
        var consumersList = new ArrayList<EventSQLConsumers>();

        System.setProperty("org.jooq.no-logo", "true");
        System.setProperty("org.jooq.no-tips", "true");

        var jooqDialect = SQLDialect.valueOf(sqlDialect.name());

        dataSources.forEach(dataSource -> {
            var dslContext = DSL.using(dataSource, jooqDialect);
            var transactions = new SQLTransactions(dslContext);

            var topicRepository = new SQLTopicRepository(transactions);
            var consumerRepository = new SQLConsumerRepository(transactions);
            var eventRepository = new SQLEventRepository(transactions, transactions);

            var registry = new DefaultEventSQLRegistry(topicRepository, eventRepository, consumerRepository, transactions);

            var topicDefinitionsCache = new TopicDefinitionsCache(topicRepository);
            var publisher = new DefaultEventSQLPublisher(topicDefinitionsCache, transactions,
                    eventRepository, flushPublishBufferSize, flushPublishBufferDelay);
            var consumers = new DefaultEventSQLConsumers(topicDefinitionsCache, transactions,
                    consumerRepository, eventRepository, publisher, clock);

            registryList.add(registry);
            publisherList.add(publisher);
            consumersList.add(consumers);
        });

        if (dataSources.size() == 1) {
            registry = registryList.getFirst();
            publisher = publisherList.getFirst();
            consumers = consumersList.getFirst();
        } else {
            registry = new ShardedEventSQLRegistry(unmodifiableList(registryList));
            publisher = new ShardedEventSQLPublisher(unmodifiableList(publisherList));
            consumers = new ShardedEventSQLConsumers(unmodifiableList(consumersList));
        }
    }

    public EventSQLRegistry registry() {
        return registry;
    }

    public EventSQLPublisher publisher() {
        return publisher;
    }

    public EventSQLConsumers consumers() {
        return consumers;
    }
}
