package com.binaryigor.eventsql.internal;

import java.time.Instant;

public record Consumer(String topic,
                       String name,
                       int partition,
                       Long firstEventId,
                       Long lastEventId,
                       Instant lastConsumptionAt,
                       long consumedEvents) {

    public Consumer withUpdatedStats(Long firstEventId, long lastEventId, Instant lastConsumptionAt, long nextConsumedEvents) {
        return new Consumer(topic, name, partition,
                this.firstEventId == null ? firstEventId : this.firstEventId,
                lastEventId, lastConsumptionAt, consumedEvents + nextConsumedEvents);
    }
}
