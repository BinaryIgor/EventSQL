package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.*;
import com.binaryigor.eventsql.internal.ConsumerRepository;
import com.binaryigor.eventsql.internal.sql.SQLConsumerRepository;
import com.binaryigor.eventsql.internal.sql.SQLEventRepository;
import com.binaryigor.eventsql.internal.sql.SQLTransactions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;

public abstract class IntegrationTest {

    protected static final PostgreSQLContainer<?> POSTGRES = postgreSQLContainer();
    protected static final DataSource dataSource;
    protected static final DSLContext dslContext;

    static {
        POSTGRES.start();
        dataSource = dataSource(POSTGRES);
        dslContext = dslContext(dataSource);
        initDbSchema(dslContext);
    }

    static PostgreSQLContainer<?> postgreSQLContainer() {
        return new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
    }

    static DataSource dataSource(PostgreSQLContainer<?> postgres) {
        var config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        return new HikariDataSource(config);
    }

    static DSLContext dslContext(DataSource dataSource) {
        return DSL.using(dataSource, SQLDialect.POSTGRES);
    }

    static void initDbSchema(DSLContext dslContext) {
        dslContext.execute("""
                CREATE TABLE topic (
                  name TEXT PRIMARY KEY,
                  partitions SMALLINT NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                
                CREATE TABLE consumer (
                  topic TEXT NOT NULL,
                  name TEXT NOT NULL,
                  partition SMALLINT NOT NULL,
                  first_event_id BIGINT,
                  last_event_id BIGINT,
                  last_consumption_at TIMESTAMP,
                  consumed_events BIGINT NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (topic, name, partition)
                );
                """);
    }

    static void cleanDb(DSLContext dslContext, EventSQLRegistry registry) {
        registry.listConsumers().forEach(c -> registry.unregisterConsumer(c.topic(), c.name()));
        registry.listTopics().forEach(t -> registry.unregisterTopic(t.name()));

        // hard to clear/drop all partitions, so let's just recreate the table each time
        dslContext.execute("""
                 DROP TABLE IF EXISTS event;
                 CREATE TABLE event (
                   topic TEXT NOT NULL,
                   id BIGSERIAL NOT NULL,
                   partition SMALLINT NOT NULL,
                   key TEXT,
                   value BYTEA NOT NULL,
                   buffered_at TIMESTAMP NOT NULL,
                   created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                   metadata JSON NOT NULL,
                   PRIMARY KEY (topic, id)
                ) PARTITION BY LIST(topic);
                
                DROP TABLE IF EXISTS event_buffer;
                CREATE TABLE event_buffer (
                  topic TEXT NOT NULL,
                  id BIGSERIAL PRIMARY KEY,
                  partition SMALLINT NOT NULL,
                  key TEXT,
                  value BYTEA NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  metadata JSON NOT NULL
                );
                
                DROP TABLE IF EXISTS event_buffer_lock;
                CREATE TABLE event_buffer_lock (
                  created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                INSERT INTO event_buffer_lock (created_at) VALUES (DEFAULT);
                """);
    }

    protected TestClock testClock;
    protected EventSQL eventSQL;
    protected EventSQLRegistry registry;
    protected EventSQLPublisher publisher;
    protected EventSQLConsumers consumers;
    protected EventSQLConsumers.DLTEventFactory dltEventFactory;
    protected SQLEventRepository eventRepository;
    protected ConsumerRepository consumerRepository;

    @BeforeEach
    protected void baseSetup() {
        testClock = new TestClock();
        eventSQL = newEventSQLInstance();
        registry = eventSQL.registry();
        publisher = eventSQL.publisher();
        consumers = eventSQL.consumers();

        dltEventFactory = eventSQL.consumers().dltEventFactory();

        var transactions = new SQLTransactions(dslContext);
        eventRepository = new SQLEventRepository(transactions, transactions, EventSQLDialect.POSTGRES);
        consumerRepository = new SQLConsumerRepository(transactions);

        cleanDb(dslContext, registry);
    }

    protected EventSQL newEventSQLInstance() {
        return new EventSQL(dataSource, EventSQLDialect.POSTGRES, testClock);
    }

    @AfterEach
    protected void baseTearDown() {
        publisher.stop(Duration.ofSeconds(3));
        consumers.stop(Duration.ofSeconds(3));
    }

    protected List<Event> publishedEvents(String topic) {
        return eventRepository.nextEvents(topic, null, null, Integer.MAX_VALUE);
    }

    protected void delay(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void flushPublishBuffer() {
        var flushed = eventRepository.flushBuffer(Short.MAX_VALUE);
        if (flushed == 0) {
            // if flush was in progress, wait arbitrary amount of time for it to finish
            delay(100);
        }
    }

    protected int eventsBufferCount() {
        return dslContext.fetchCount(DSL.table("event_buffer"));
    }
}
