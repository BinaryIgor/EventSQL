package com.binaryigor.eventsql.internal.sharded;

import com.binaryigor.eventsql.ConsumerDefinition;
import com.binaryigor.eventsql.EventSQLRegistry;
import com.binaryigor.eventsql.TopicDefinition;

import java.util.List;

public class ShardedEventSQLRegistry implements EventSQLRegistry {

    private final List<EventSQLRegistry> registries;

    public ShardedEventSQLRegistry(List<EventSQLRegistry> registries) {
        this.registries = registries;
    }

    @Override
    public EventSQLRegistry registerTopic(TopicDefinition topic) {
        registries.forEach(r -> r.registerTopic(topic));
        return this;
    }

    @Override
    public EventSQLRegistry unregisterTopic(String topic) {
        registries.forEach(r -> r.unregisterTopic(topic));
        return this;
    }

    @Override
    public List<TopicDefinition> listTopics() {
        return registries.getFirst().listTopics();
    }

    @Override
    public EventSQLRegistry registerConsumer(ConsumerDefinition consumer) {
        registries.forEach(r -> r.registerConsumer(consumer));
        return this;
    }

    @Override
    public EventSQLRegistry unregisterConsumer(String topic, String name) {
        registries.forEach(r -> r.unregisterConsumer(topic, name));
        return this;
    }

    @Override
    public List<ConsumerDefinition> listConsumers() {
        return registries.getFirst().listConsumers();
    }

    @Override
    public void configureTablesManager(TablesManager tablesManager) {
        registries.forEach(r -> r.configureTablesManager(tablesManager));
    }

    @Override
    public TablesManager tablesManager() {
        return registries.getFirst().tablesManager();
    }
}
