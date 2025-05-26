package com.binaryigor.eventsql;

import java.util.List;

public interface EventSQLRegistry {

    EventSQLRegistry registerTopic(TopicDefinition topic);

    EventSQLRegistry unregisterTopic(String topic);

    List<TopicDefinition> listTopics();

    EventSQLRegistry registerConsumer(ConsumerDefinition consumer);

    EventSQLRegistry unregisterConsumer(String topic, String name);

    List<ConsumerDefinition> listConsumers();
    
    void configureTablesManager(TablesManager tablesManager);
    
    TablesManager tablesManager();

    // all methods must be idempotent
    interface TablesManager {
        
        void prepareTopicTable();
        
        void prepareConsumerTable();
        
        void prepareEventTable(String topic);
        
        void dropEventTable(String topic);
    }
}
