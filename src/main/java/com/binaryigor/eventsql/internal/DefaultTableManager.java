package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.EventSQLRegistry;

public class DefaultTableManager implements EventSQLRegistry.TableManager {

    private final TopicRepository topicRepository;
    private final ConsumerRepository consumerRepository;
    private final EventRepository eventRepository;

    public DefaultTableManager(TopicRepository topicRepository, ConsumerRepository consumerRepository, EventRepository eventRepository) {
        this.topicRepository = topicRepository;
        this.consumerRepository = consumerRepository;
        this.eventRepository = eventRepository;
    }

    @Override
    public void prepareTopicTable() {
        topicRepository.createTable();
    }

    @Override
    public void prepareConsumerTable() {
        consumerRepository.createTable();
    }

    @Override
    public void prepareEventTable(String topic) {
        eventRepository.prepareBufferLock();
        eventRepository.createBuffer();
        eventRepository.createPartition(topic);
    }

    @Override
    public void dropEventTable(String topic) {
        eventRepository.dropPartition(topic);
    }
}
