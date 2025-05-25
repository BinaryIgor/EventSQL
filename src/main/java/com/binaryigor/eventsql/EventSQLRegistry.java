package com.binaryigor.eventsql;

import java.util.List;

public interface EventSQLRegistry {

    EventSQLRegistry registerTopic(TopicDefinition topic);

    EventSQLRegistry unregisterTopic(String topic);

    List<TopicDefinition> listTopics();

    EventSQLRegistry registerConsumer(ConsumerDefinition consumer);

    EventSQLRegistry unregisterConsumer(String topic, String name);

    List<ConsumerDefinition> listConsumers();
    
    void configureTableManager(TableManager tableManager);
    
    TableManager tableManager();

    // all methods must be idempotent
    interface TableManager {
        
        void prepareTopicTable();
        
        void prepareConsumerTable();
        
        void prepareEventTable(String topic);
        
        void dropEventTable(String topic);
    }
}
