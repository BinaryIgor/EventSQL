package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class EventSQLBenchmarksRunner {

    static final Random RANDOM = new Random();
    static final String DB_URL = envValueOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/events");
    static final String DB_USERNAME = envValueOrDefault("DB_URL", "events");
    static final String DB_PASSWORD = envValueOrDefault("DB_PASSWORD", "events");
    static final int DATA_SOURCE_POOL_SIZE = envIntValueOrDefault("DATA_SOURCE_POOL_SIZE", 50);
    static final EventSQLDialect SQL_DIALECT;
    static final int RUNNER_INSTANCES = envIntValueOrDefault("RUNNER_INSTANCES", 1);
    static final int EVENTS_TO_PUBLISH = envIntValueOrDefault("EVENTS_TO_PUBLISH", 60_000);
    static final int EVENTS_BATCH_SIZE = envIntValueOrDefault("EVENTS_BATCH_SIZE", 1);
    static final int EVENTS_RATE = envIntValueOrDefault("EVENTS_RATE", 1_000);
    static final String TEST_TOPIC = envValueOrDefault("TEST_TOPIC", "account_created");
    static final String TEST_CONSUMER = envValueOrDefault("TEST_CONSUMER", "benchmarks-consumer");

    static {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        var sqlDialect = envValueOrDefault("SQL_DIALECT", null);
        if (sqlDialect == null) {
            if (DB_URL.contains("postgres")) {
                SQL_DIALECT = EventSQLDialect.POSTGRES;
            } else if (DB_URL.contains("mysql")) {
                SQL_DIALECT = EventSQLDialect.MYSQL;
            } else {
                SQL_DIALECT = EventSQLDialect.MARIADB;
            }
        } else {
            SQL_DIALECT = EventSQLDialect.valueOf(sqlDialect);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting EventSQL benchmark, connecting to events db...");

        var dataSource = dataSource(DB_URL, DB_USERNAME, DB_PASSWORD);

        var eventSQL = new EventSQL(dataSource, SQL_DIALECT);

        printDelimiter();

        System.out.printf("Connection established, about to publish %d events to %s topic with %s per second rate%n", EVENTS_TO_PUBLISH, TEST_TOPIC, EVENTS_RATE);
        if (RUNNER_INSTANCES > 1) {
            printDelimiter();
            System.out.printf("%d runner instances are running in parallel, so the real rate will be %d per second for %d events%n",
                    RUNNER_INSTANCES, RUNNER_INSTANCES * EVENTS_RATE, RUNNER_INSTANCES * EVENTS_TO_PUBLISH);
            printDelimiter();
        }
        var topicDefinition = topicDefinition(eventSQL);
        var consumerDefinition = consumerDefinition(eventSQL);
        printDelimiter();
        System.out.println("TopicDefinition: " + topicDefinition);
        System.out.println("ConsumerDefinition: " + consumerDefinition);
        printDelimiter();

        var start = System.currentTimeMillis();

        publishEvents(eventSQL.publisher());
        waitForPublishBufferToEmpty(dataSource);
        var publicationDuration = Duration.ofMillis(System.currentTimeMillis() - start);

        printDelimiter();
        var publicationPerSecondRate = perSecondRate(publicationDuration);
        System.out.printf("Publishing %d events with %d per second rate took: %s, which means %d per second rate%n",
                EVENTS_TO_PUBLISH, EVENTS_RATE, publicationDuration, publicationPerSecondRate);
        if (RUNNER_INSTANCES > 1) {
            System.out.printf("%d runner instances were running in parallel, so the real rate was %d per second for %d events%n",
                    RUNNER_INSTANCES, RUNNER_INSTANCES * publicationPerSecondRate, RUNNER_INSTANCES * EVENTS_TO_PUBLISH);
        }
        printDelimiter();
        System.out.println("Waiting for consumption....");
        printDelimiter();

        waitForConsumers(dataSource, consumerDefinition);
        var consumptionDuration = Duration.ofMillis(System.currentTimeMillis() - start);

        printDelimiter();
        var consumptionPerSecondRate = perSecondRate(consumptionDuration);
        System.out.printf("Consuming %d events with %d per second rate took: %s, which means %d per second rate%n",
                EVENTS_TO_PUBLISH, EVENTS_RATE, consumptionDuration, consumptionPerSecondRate);
        if (RUNNER_INSTANCES > 1) {
            System.out.printf("%d runner instances were running in parallel, so the real rate was %d per second for %d events%n",
                    RUNNER_INSTANCES, RUNNER_INSTANCES * consumptionPerSecondRate, RUNNER_INSTANCES * EVENTS_TO_PUBLISH);
        }
        printDelimiter();
    }

    static String envValueOrDefault(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }

    static int envIntValueOrDefault(String key, int defaultValue) {
        return Integer.parseInt(envValueOrDefault(key, String.valueOf(defaultValue)));
    }

    static void printDelimiter() {
        System.out.println();
        System.out.println("...");
        System.out.println();
    }

    static DataSource dataSource(String jdbcUrl, String username, String password) {
        var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(DATA_SOURCE_POOL_SIZE);
        config.setMaximumPoolSize(DATA_SOURCE_POOL_SIZE);
        return new HikariDataSource(config);
    }

    static TopicDefinition topicDefinition(EventSQL eventSQL) {
        return eventSQL.registry().listTopics().stream()
                .filter(t -> t.name().equals(TEST_TOPIC))
                .findFirst()
                .orElseThrow();
    }

    static ConsumerDefinition consumerDefinition(EventSQL eventSQL) {
        return eventSQL.registry().listConsumers().stream()
                .filter(c -> c.topic().equals(TEST_TOPIC) && c.name().equals(TEST_CONSUMER))
                .findFirst()
                .orElseThrow();
    }

    static EventTableStats eventTableStats(DataSource source) {
        return executeQuery(source, "SELECT eql_partition, MAX(eql_id) FROM %s_event GROUP BY eql_partition"
                .formatted(TEST_TOPIC), r -> {
            var lastIdsPerPartition = new HashMap<Integer, Long>();
            while (r.next()) {
                lastIdsPerPartition.put(r.getInt(1), r.getLong(2));
            }
            var maxId = lastIdsPerPartition.values().stream().mapToLong(e -> e).max().orElseThrow();
            return new EventTableStats(maxId, lastIdsPerPartition);
        });
    }

    static ConsumerTableStats consumerTableStats(DataSource source) {
        return executeQuery(source, """
                SELECT eql_partition, MAX(eql_last_event_id) FROM consumer
                WHERE eql_topic = '%s' AND eql_name = '%s'
                GROUP BY eql_partition"""
                .formatted(TEST_TOPIC, TEST_CONSUMER), r -> {
            var lastIdsPerPartition = new HashMap<Integer, Long>();
            while (r.next()) {
                lastIdsPerPartition.put((int) r.getShort(1), r.getLong(2));
            }
            return new ConsumerTableStats(lastIdsPerPartition);
        });
    }

    static <T> T executeQuery(DataSource source, String query, ResultSetMapper<T> resultMapper) {
        try (var conn = source.getConnection()) {
            var statement = conn.createStatement();
            var result = statement.execute(query);
            if (result) {
                var resultSet = statement.getResultSet();
                return resultMapper.map(resultSet);
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void publishEvents(EventSQLPublisher publisher) throws Exception {
        var futures = new LinkedList<Future<?>>();

        String eventsWerePublishedMessage;
        if (EVENTS_BATCH_SIZE > 1) {
            eventsWerePublishedMessage = "events were published - in batches of " + EVENTS_BATCH_SIZE;
        } else {
            eventsWerePublishedMessage = "events were published";
        }

        var publishRate = EVENTS_RATE / EVENTS_BATCH_SIZE;
        var batchesToPublish = EVENTS_TO_PUBLISH / EVENTS_BATCH_SIZE;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var i = 0; i < batchesToPublish; i++) {
                var result = executor.submit(() -> publishRandomEventOrEventsBatch(publisher));
                futures.add(result);

                var publications = i + 1;
                if (futures.size() >= publishRate && publications < batchesToPublish) {
                    System.out.printf("%s, %d/%d %s, waiting 1s before next publications...%n",
                            LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS),
                            publications * EVENTS_BATCH_SIZE,
                            EVENTS_TO_PUBLISH,
                            eventsWerePublishedMessage);
                    Thread.sleep(1000);
                    awaitForFutures(futures);
                    futures.clear();
                }
            }

            if (!futures.isEmpty()) {
                awaitForFutures(futures);
                futures.clear();
            }
        }
    }

    static void publishRandomEventOrEventsBatch(EventSQLPublisher publisher) {
        try {
            // make publication more evenly distributed in time
            Thread.sleep(RANDOM.nextInt(1000));
            if (EVENTS_BATCH_SIZE > 1) {
                var batch = Stream.generate(EventSQLBenchmarksRunner::nextEvent)
                        .limit(EVENTS_BATCH_SIZE)
                        .toList();
                publisher.publishAll(batch);
            } else {
                publisher.publish(nextEvent());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static EventPublication nextEvent() {
        return new EventPublication(TEST_TOPIC, accountCreatedEventJson().getBytes(StandardCharsets.UTF_8));
    }

    static String accountCreatedEventJson() {
        var id = UUID.randomUUID().toString();
        return """
                {
                  "id": "%s",
                  "email": "%s",
                  "name": "%s",
                  "createdAt": "%s"
                }
                """.formatted(id,
                id + "@email.com",
                id + "-name",
                Instant.now().minusSeconds(RANDOM.nextLong(24 * 60 * 60)).toString());
    }

    static void awaitForFutures(List<Future<?>> futures) {
        futures.forEach(r -> {
            try {
                r.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    static void waitForPublishBufferToEmpty(DataSource dataSource) {
        while (true) {
            try {
                var bufferSize = executeQuery(dataSource, "SELECT COUNT(*) FROM event_buffer", r -> {
                    if (r.next()) {
                        return r.getInt(1);
                    }
                    return 0;
                });
                if (bufferSize == 0) {
                    break;
                }
                Thread.sleep(250);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void waitForConsumers(DataSource dataSource, ConsumerDefinition consumerDefinition) throws Exception {
        var eventsStats = eventTableStats(dataSource);
        while (true) {
            var consumersStats = consumerTableStats(dataSource);
            var consumersFinished = false;

            if (consumerDefinition.partitioned()) {
                consumersFinished = true;
                for (var e : consumersStats.lastIdsPerPartition().entrySet()) {
                    var consumerPartition = e.getKey();
                    var lastConsumerEventId = e.getValue();
                    var lastEventId = eventsStats.lastIdsPerPartition().get(consumerPartition);
                    if (lastConsumerEventId == null || lastEventId > lastConsumerEventId) {
                        consumersFinished = false;
                        System.out.printf("Consumer of %d partition is at the event %d, but latest event is %d; waiting for 1s...%n", consumerPartition, lastConsumerEventId, lastEventId);
                        break;
                    }
                }
            } else {
                var lastConsumerEventId = consumersStats.lastIdsPerPartition().get(-1);
                consumersFinished = lastConsumerEventId >= eventsStats.lastId();
                System.out.printf("Consumer is at %d event, but latest event is %d; waiting for 1s...%n", lastConsumerEventId, eventsStats.lastId());
            }

            if (consumersFinished) {
                break;
            }

            Thread.sleep(500);
        }
    }

    static int perSecondRate(Duration duration) {
        return BigDecimal.valueOf(EVENTS_TO_PUBLISH * 1000.0 / duration.toMillis())
                .setScale(1, RoundingMode.HALF_UP)
                .intValue();
    }

    record EventTableStats(long lastId,
                           Map<Integer, Long> lastIdsPerPartition) {
        EventTableStats {
            if (lastIdsPerPartition == null || lastIdsPerPartition.isEmpty()) {
                throw new IllegalArgumentException("lastIdsPerPartition cannot be null or empty");
            }
        }
    }

    record ConsumerTableStats(Map<Integer, Long> lastIdsPerPartition) {
        ConsumerTableStats {
            if (lastIdsPerPartition == null || lastIdsPerPartition.isEmpty()) {
                throw new IllegalArgumentException("lastIdsPerPartition cannot be null or empty");
            }
        }
    }

    interface ResultSetMapper<T> {
        T map(ResultSet result) throws Exception;
    }
}
