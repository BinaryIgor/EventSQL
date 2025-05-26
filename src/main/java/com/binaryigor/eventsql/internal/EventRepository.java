package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.Event;

import java.util.Collection;
import java.util.List;

public interface EventRepository {

    void createBuffer();

    void createPartition(String topic);

    void dropPartition(String topic);

    void create(EventInput event);

    void createAll(Collection<EventInput> events);

    int flushBuffer(Collection<String> topics, int toFlush);

    List<Event> nextEvents(String topic, Long lastId, int limit);

    List<Event> nextEvents(String topic, int partition, Long lastId, int limit);
}
