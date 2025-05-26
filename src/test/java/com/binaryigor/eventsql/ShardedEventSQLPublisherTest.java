package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.ShardedIntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import com.binaryigor.eventsql.test.TestPartitioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ShardedEventSQLPublisherTest extends ShardedIntegrationTest {

    private static final String PARTITIONED_TOPIC = "partitioned_topic";

    @BeforeEach
    void setup() {
        registry.registerTopic(new TopicDefinition(PARTITIONED_TOPIC, 3));
    }

    @Test
    void publishesToAllShards() {
        // when
        var toPublishEvents = 50;
        IntStream.range(0, toPublishEvents)
                .forEach($ -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC)));
        flushPublishBuffers(PARTITIONED_TOPIC);

        // then
        var allPublishedEvents = new AtomicInteger();
        assertOnEachShard(shard -> {
            var publishedEvents = publishedEvents(shard, PARTITIONED_TOPIC);
            assertThat(publishedEvents).isNotEmpty();
            allPublishedEvents.addAndGet(publishedEvents.size());
        });
        assertThat(allPublishedEvents.get())
                .isEqualTo(toPublishEvents);
    }

    @Test
    void publishesBatchesToAllShards() {
        // when
        var batchSize = 5;
        var toPublishEvents = batchSize * 10;
        IntStream.range(0, 10)
                .forEach($ -> {
                    var batch = Stream.generate(() -> TestObjects.randomEventPublication(PARTITIONED_TOPIC))
                            .limit(batchSize)
                            .toList();
                    publisher.publishAll(batch);
                });
        flushPublishBuffers(PARTITIONED_TOPIC);

        // then
        var allPublishedEvents = new AtomicInteger();
        assertOnEachShard(shard -> {
            var publishedEvents = publishedEvents(shard, PARTITIONED_TOPIC);
            assertThat(publishedEvents).isNotEmpty();
            allPublishedEvents.addAndGet(publishedEvents.size());
        });
        assertThat(allPublishedEvents.get())
                .isEqualTo(toPublishEvents);
    }

    @Test
    void configuresPartitionerForAllShardPublishers() {
        // given
        var shardPublishers = publisher.publishers();

        // when
        var configuredPartitioner = new TestPartitioner(0);
        publisher.configurePartitioner(configuredPartitioner);

        // then
        shardPublishers.forEach(p ->
                assertThat(p.partitioner())
                        .isEqualTo(configuredPartitioner));
    }

    private void assertOnEachShard(Consumer<Integer> shardAssertion) {
        IntStream.range(0, SHARDS)
                .forEach(shardAssertion::accept);
    }
}
