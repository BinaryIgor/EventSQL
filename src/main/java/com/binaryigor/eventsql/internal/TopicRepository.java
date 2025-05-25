package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.TopicDefinition;

import java.util.List;
import java.util.Optional;

public interface TopicRepository {

    void createTable();

    void save(TopicDefinition topic);

    Optional<TopicDefinition> ofName(String name);

    List<TopicDefinition> all();

    void delete(String topic);
}
