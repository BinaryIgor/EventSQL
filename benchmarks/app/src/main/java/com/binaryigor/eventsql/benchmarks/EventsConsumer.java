package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class EventsConsumer {

    private static final Random RANDOM = new Random();
    private static final Logger logger = LoggerFactory.getLogger(EventsConsumer.class);
    private final EventSQL eventSQL;
    private final ObjectMapper objectMapper;
    private final EventsProperties eventsProperties;
    private final Map<String, Counter> accountHandledCountersByConsumer;

    public EventsConsumer(EventSQL eventSQL,
                          ObjectMapper objectMapper,
                          EventsProperties eventsProperties,
                          MeterRegistry meterRegistry) {
        this.eventSQL = eventSQL;
        this.objectMapper = objectMapper;
        this.eventsProperties = eventsProperties;

        this.accountHandledCountersByConsumer = eventsProperties.accountCreatedConsumers().stream()
                .collect(Collectors.toMap(ConsumerDefinition::name,
                        c -> Counter.builder("accounts_handled_total")
                                .tag("consumer", c.name())
                                .register(meterRegistry)));
    }

    @PostConstruct
    void start() {
        eventSQL.registry()
                .registerTopic(eventsProperties.accountCreatedTopic())
                .registerTopic(eventsProperties.accountCreatedTopicDlt());

        eventsProperties.accountCreatedConsumers()
                .forEach(c -> {
                    eventSQL.registry().registerConsumer(c);
                    eventSQL.consumers()
                            .startBatchConsumer(c.topic(), c.name(),
                                    events -> handleAccountCreatedEvents(c.name(), events),
                                    EventSQLConsumers.ConsumptionConfig.of(10, 100));
                });
    }

    @PreDestroy
    void stop() {
        eventSQL.consumers().stop(Duration.ofSeconds(5));
    }

    private void handleAccountCreatedEvents(String consumer, List<Event> events) {
        try {
            var accountsCreated = new ArrayList<AccountCreated>(events.size());
            for (var e : events) {
                accountsCreated.add(objectMapper.readValue(e.value(), AccountCreated.class));
            }
            handleDelay();
            accountHandledCountersByConsumer.get(consumer)
                    .increment(accountsCreated.size());
        } catch (IllegalStateException e) {
            var firstFailedEvent = events.getFirst();
            throw new EventSQLConsumptionException(e, firstFailedEvent);
        } catch (Exception e) {
            logger.error("Problem while handling AccountCreated events:", e);
            throw new RuntimeException(e);
        }
    }

    private void handleDelay() {
        try {
            Thread.sleep(1 + RANDOM.nextInt(100));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int handledEventsTotal(String consumer) {
        return Optional.ofNullable(accountHandledCountersByConsumer.get(consumer))
                .map(c -> (int) c.count())
                .orElse(0);
    }
}
