package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.ConsumerDefinition;
import com.binaryigor.eventsql.EventSQLDialect;
import com.binaryigor.eventsql.TopicDefinition;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties("events")
public record EventsProperties(TopicDefinition accountCreatedTopic,
                               TopicDefinition accountCreatedTopicDlt,
                               List<ConsumerDefinition> accountCreatedConsumers,
                               List<DataSourceProperties> dataSources,
                               EventSQLDialect sqlDialect) {

    record DataSourceProperties(String url,
                                String username,
                                String password,
                                int connections,
                                boolean enabled) {
    }
}
