package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.ShardedIntegrationTest;
import com.binaryigor.eventsql.test.TestDLTEventFactory;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.binaryigor.eventsql.test.Tests.awaitAssertion;
import static org.assertj.core.api.Assertions.assertThat;

public class ShardedEventSQLConsumersTest extends ShardedIntegrationTest {

    private static final String TOPIC = "topic";
    private static final String PARTITIONED_TOPIC = "partitioned_topic";
    private static final Duration CONSUMER_POLLING_DELAY = Duration.ofMillis(10);

    @BeforeEach
    void setup() {
        registry.registerTopic(new TopicDefinition(TOPIC, -1))
                .registerTopic(new TopicDefinition(PARTITIONED_TOPIC, 3));
    }

    @Test
    void consumesEventsFromTopicOnAllShards() {
        // given
        var consumer = new ConsumerDefinition(TOPIC, "test-consumer", false);
        registry.registerConsumer(consumer);

        var events = Stream.generate(() -> TestObjects.randomEventPublication(TOPIC))
                .limit(10)
                .toList();
        var capturedEvents = new ArrayList<Event>();

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), capturedEvents::add, CONSUMER_POLLING_DELAY);
        events.forEach(publisher::publish);
        flushPublishBuffers(TOPIC);

        // then
        awaitAssertion(() -> assertExpectedEvents(capturedEvents, events));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void consumesEventsFromPartitionedTopicOnAllShards(boolean nullKeys) {
        // given
        var consumer = new ConsumerDefinition(PARTITIONED_TOPIC, "test-consumer", true);
        registry.registerConsumer(consumer);

        var events = Stream.generate(() -> {
                    var key = nullKeys ? null : UUID.randomUUID().toString();
                    return TestObjects.randomEventPublication(PARTITIONED_TOPIC, key);
                }).limit(50)
                .toList();
        var p0CapturedEventsTotal = new AtomicInteger();
        var p1CapturedEventsTotal = new AtomicInteger();
        var p2CapturedEventsTotal = new AtomicInteger();
        var capturedEvents = new CopyOnWriteArrayList<Event>();

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), e -> {
            capturedEvents.add(e);
            if (e.partition() == 0) {
                p0CapturedEventsTotal.incrementAndGet();
            } else if (e.partition() == 1) {
                p1CapturedEventsTotal.incrementAndGet();
            } else {
                p2CapturedEventsTotal.incrementAndGet();
            }
        }, CONSUMER_POLLING_DELAY);
        events.forEach(publisher::publish);
        flushPublishBuffers(PARTITIONED_TOPIC);

        // then
        awaitAssertion(() -> {
            assertThat(p0CapturedEventsTotal).hasPositiveValue();
            assertThat(p1CapturedEventsTotal).hasPositiveValue();
            assertThat(p2CapturedEventsTotal).hasPositiveValue();
            assertExpectedEvents(capturedEvents, events);
        });
    }

    @Test
    void configuresDltFactoryForAllShardConsumers() {
        // given
        var shardConsumers = consumers.consumers();

        // when
        var configuredDltFactory = new TestDLTEventFactory();
        consumers.configureDLTEventFactory(configuredDltFactory);

        // then
        shardConsumers.forEach(c ->
                assertThat(c.dltEventFactory())
                        .isEqualTo(configuredDltFactory));
    }

    private void assertExpectedEvents(Collection<Event> capturedEvents, List<EventPublication> expectations) {
        assertThat(capturedEvents)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "partition")
                .containsExactlyInAnyOrderElementsOf(expectedEvents(expectations));
    }

    private List<Event> expectedEvents(List<EventPublication> publications) {
        return publications.stream()
                .map(this::toEvent)
                .toList();
    }

    private Event toEvent(EventPublication publication) {
        return new Event(publication.topic(), -1, -1, publication.key(), publication.value(),
                publication.metadata());
    }
}
