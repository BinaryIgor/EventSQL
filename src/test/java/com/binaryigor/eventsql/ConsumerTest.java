package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.Consumer;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerTest {

    @Test
    void returnsConsumerWithUpdatedStats() {
        var now = Instant.now();
        var initialConsumer = new Consumer("some-topic", "some-name", 1,
                null, null, now, 0);

        var updatedConsumer1 = initialConsumer.withUpdatedStats(null, 2, now.plusSeconds(1), 1);
        assertThat(updatedConsumer1)
                .isEqualTo(expectedUpdatedConsumer(null, 2, now.plusSeconds(1), 1));

        var updatedConsumer2 = updatedConsumer1.withUpdatedStats(3L, 9L, now.plusSeconds(10), 1);
        assertThat(updatedConsumer2)
                .isEqualTo(expectedUpdatedConsumer(3L, 9L, now.plusSeconds(10), 2));

        var updatedConsumer3 = updatedConsumer2.withUpdatedStats(99L, 100L, now.plusSeconds(100), 3);
        assertThat(updatedConsumer3)
                .isEqualTo(expectedUpdatedConsumer(3L, 100L, now.plusSeconds(100), 5));
    }

    private Consumer expectedUpdatedConsumer(Long firstEventId, long lastEventId, Instant lastConsumptionAt, int nextConsumedEvents) {
        return new Consumer("some-topic", "some-name", 1,
                firstEventId, lastEventId, lastConsumptionAt, nextConsumedEvents);
    }
}
