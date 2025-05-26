package com.binaryigor.eventsql.internal;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface ConsumerRepository {

    void createTable();

    void save(Consumer consumer);

    void saveAll(Collection<Consumer> consumers);

    List<Consumer> all();

    List<Consumer> allOf(String topic, String name);

    List<Consumer> allOf(String topic);

    Optional<Consumer> ofIdForUpdateSkippingLocked(ConsumerId id);

    void update(Consumer consumer);

    void deleteAllOf(String topic, String name);
}
