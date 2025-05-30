package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.DefaultPartitioner;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultPartitionerTest {

    private final DefaultPartitioner partitioner = new DefaultPartitioner();

    @Test
    void assignsEvenlyDistributedRandomPartitionsForEventWithNullKey() {
        // given
        var event = TestObjects.randomEventPublication("some_topic", null);
        var topicPartitions = 3;

        // when
        var assignedPartitions = Stream.generate(() -> partitioner.partition(event, topicPartitions))
                .limit(25)
                .collect(Collectors.toSet());

        // then
        assertThat(assignedPartitions).hasSize(3);
        assignedPartitions.forEach(partition ->
                assertThat(partition)
                        .isGreaterThanOrEqualTo(0)
                        .isLessThan(topicPartitions));
    }

    @Test
    void assignsEvenlyDistributedStickyPartitionsForEventsWithDefinedKeys() {
        // given
        var eventsByKey = Stream.generate(() -> TestObjects.randomEventPublication("some_topic", UUID.randomUUID().toString()))
                .limit(50)
                .collect(Collectors.toMap(EventPublication::key, Function.identity()));
        var topicPartitions = 5;

        // when
        var assignedPartitionsByKey = eventsByKey.values().stream()
                .collect(Collectors.toMap(EventPublication::key,
                        e -> partitioner.partition(e, topicPartitions)));

        // then all partitions were used
        assertThat(assignedPartitionsByKey.values())
                .containsOnly(0, 1, 2, 3, 4);
        // and then partitioner always returns the same partition for a given key
        assignedPartitionsByKey.forEach((key, partition) ->
                IntStream.range(0, 5).forEach($ -> {
                    var event = eventsByKey.get(key);
                    var nextPartition = partitioner.partition(event, topicPartitions);
                    assertThat(nextPartition).isEqualTo(partition);
                }));
    }
}
