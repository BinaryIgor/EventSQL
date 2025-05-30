package com.binaryigor.eventsql.support;

import com.binaryigor.eventsql.*;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.binaryigor.eventsql.test.IntegrationTestSupport.dataSource;
import static com.binaryigor.eventsql.test.Tests.awaitAssertion;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class DBSupportTest {

    private static final Random RANDOM = new Random();
    private static final String TOPIC = "topic";
    private final JdbcDatabaseContainer<?> databaseContainer;
    private final EventSQLDialect sqlDialect;

    private EventSQLRegistry registry;
    private EventSQLPublisher publisher;
    private EventSQLConsumers consumers;

    protected DBSupportTest(JdbcDatabaseContainer<?> databaseContainer, EventSQLDialect sqlDialect) {
        this.databaseContainer = databaseContainer;
        this.sqlDialect = sqlDialect;
    }

    @BeforeEach
    void setup() {
        databaseContainer.start();

        var eventSQL = new EventSQL(dataSource(databaseContainer), sqlDialect);
        registry = eventSQL.registry();
        publisher = eventSQL.publisher();
        consumers = eventSQL.consumers();
    }

    @AfterEach
    void tearDown() {
        databaseContainer.stop();
    }

    @Test
    void publishesAndConsumesEventsToAndFromTopic() {
        // given
        registry.registerTopic(new TopicDefinition(TOPIC, -1));
        var consumer = new ConsumerDefinition(TOPIC, "test-consumer", false);
        registry.registerConsumer(consumer);
        // and
        var events = List.of(
                TestObjects.randomEventPublication(TOPIC, "key1"),
                TestObjects.randomEventPublication(TOPIC, "key2"),
                TestObjects.randomEventPublication(TOPIC, "key3"),
                TestObjects.randomEventPublication(TOPIC, "key4"));
        var capturedEvents = new CopyOnWriteArrayList<Event>();

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), capturedEvents::add,
                Duration.ofMillis(100));
        events.forEach(e -> {
            randomDelay();
            publisher.publish(e);
        });

        // then
        awaitAssertion(() -> {
            assertThat(capturedEvents)
                    .hasSize(events.size())
                    .extracting("key")
                    .containsExactly("key1", "key2", "key3", "key4");
        });
    }

    private void randomDelay() {
        try {
            Thread.sleep(1 + RANDOM.nextInt(100));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
