package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.Event;

import java.util.Collection;
import java.util.List;

public interface EventRepository {

    void createPartition(String topic);

    void deletePartition(String topic);

    void create(EventInput event);

    void createAll(Collection<EventInput> events);

    int flushBuffer(int toFlush);

    List<Event> nextEvents(String topic, Long lastId, int limit);

    List<Event> nextEvents(String topic, int partition, Long lastId, int limit);
}
