package com.binaryigor.eventsql;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface EventSQLConsumers {

    Duration DEFAULT_POLLING_DELAY = Duration.ofMillis(500);
    int DEFAULT_IN_MEMORY_EVENTS = 10;

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer);

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer,
                       Duration pollingDelay);

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer,
                       Duration pollingDelay,
                       int maxInMemoryEvents);

    void startBatchConsumer(String topic, String name,
                            Consumer<List<Event>> consumer,
                            ConsumptionConfig consumptionConfig);

    void configureDLTEventFactory(DLTEventFactory dltEventFactory);

    DLTEventFactory dltEventFactory();

    void stop(Duration timeout);

    record ConsumptionConfig(
            int minEvents,
            int maxEvents,
            Duration pollingDelay,
            Duration maxPollingDelay) {

        public static ConsumptionConfig of(int minEvents,
                                           int maxEvents,
                                           Duration pollingDelay,
                                           Duration maxPollingDelay) {
            return new ConsumptionConfig(minEvents, maxEvents, pollingDelay, maxPollingDelay);
        }

        public static ConsumptionConfig of(int minEvents, int maxEvents) {
            return of(minEvents, maxEvents, DEFAULT_POLLING_DELAY, DEFAULT_POLLING_DELAY.multipliedBy(2));
        }
    }

    interface DLTEventFactory {
        Optional<EventPublication> create(EventSQLConsumptionException exception, String consumer);
    }
}
