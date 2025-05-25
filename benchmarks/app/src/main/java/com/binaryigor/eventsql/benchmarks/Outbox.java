package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLDialect;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.time.Duration;

public class Outbox {

    private final EventSQL eventSQL;
    private final ObjectMapper objectMapper;

    public Outbox(DataSource dataSource, EventSQLDialect sqlDialect, ObjectMapper objectMapper) {
        this.eventSQL = new EventSQL(dataSource, sqlDialect);
        this.objectMapper = objectMapper;
    }

    public <E> void publish(Event<E> event) {
        eventSQL.publisher().publish(toPublication(event));
    }

    public EventSQLConsumers consumers() {
        return eventSQL.consumers();
    }

    private <E> EventPublication toPublication(Event<E> event) {
        try {
            var value = objectMapper.writeValueAsBytes(event.value);
            return new EventPublication(event.topic, event.key, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stop(Duration timeout) {
        eventSQL.publisher().stop(timeout);
        eventSQL.consumers().stop(timeout);
    }

    public record Event<E>(String topic, String key, E value) {
        public Event(String topic, E value) {
            this(topic, null, value);
        }
    }
}
