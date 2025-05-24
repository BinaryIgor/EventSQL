package com.binaryigor.eventsql;

import java.time.Duration;
import java.util.Collection;

public interface EventSQLPublisher {

    void publish(EventPublication publication);

    void publishAll(Collection<EventPublication> publications);

    void configurePartitioner(Partitioner partitioner);

    Partitioner partitioner();

    void stop(Duration timeout);

    interface Partitioner {
        int partition(EventPublication publication, int topicPartitions);
    }
}
