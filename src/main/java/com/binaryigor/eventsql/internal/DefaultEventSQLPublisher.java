package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQLPublisher;
import com.binaryigor.eventsql.TopicDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

// TODO: support topic definitions reloading
public class DefaultEventSQLPublisher implements EventSQLPublisher {

    private static final Logger logger = LoggerFactory.getLogger(DefaultEventSQLPublisher.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final TopicDefinitionsCache topicDefinitionsCache;
    private final Transactions transactions;
    private final EventRepository eventRepository;
    private Partitioner partitioner;
    private final AtomicBoolean published = new AtomicBoolean(false);
    private final Set<String> publishedTopics = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicBoolean flushPublishBufferThreadSet = new AtomicBoolean(false);
    private volatile Thread flushPublishBufferThread;
    private final int flushPublishBufferSize;
    private final Duration flushPublishBufferDelay;

    public DefaultEventSQLPublisher(TopicDefinitionsCache topicDefinitionsCache,
                                    Transactions transactions,
                                    EventRepository eventRepository,
                                    int flushPublishBufferSize,
                                    Duration flushPublishBufferDelay) {
        this.topicDefinitionsCache = topicDefinitionsCache;
        this.transactions = transactions;
        this.eventRepository = eventRepository;
        this.flushPublishBufferSize = flushPublishBufferSize;
        this.flushPublishBufferDelay = flushPublishBufferDelay;
        this.partitioner = new DefaultPartitioner();
    }

    @Override
    public void publish(EventPublication publication) {
        publish(publication.topic(), List.of(publication));
    }

    @Override
    public void publishAll(Collection<EventPublication> publications) {
        var publicationsByTopic = publications.stream()
                .collect(Collectors.groupingBy(EventPublication::topic));
        transactions.execute(() -> publicationsByTopic.forEach(this::publish));
    }

    private void publish(String topicName, Collection<EventPublication> publications) {
        var topic = findTopicDefinition(topicName);
        var toCreateEvents = publications.stream()
                .map(publication -> {
                    var partition = (short) partitioner.partition(publication, topic.partitions());
                    var eventInput = new EventInput(publication, partition);
                    validateNewEvent(eventInput, topic);
                    return eventInput;
                })
                .toList();
        eventRepository.createAll(toCreateEvents);

        published.set(true);
        publishedTopics.add(topicName);

        var shouldSetFlushPublishBufferThread = !flushPublishBufferThreadSet.compareAndExchange(false, true);
        if (shouldSetFlushPublishBufferThread) {
            startFlushPublishBufferThread();
        }
    }

    private void startFlushPublishBufferThread() {
        flushPublishBufferThread = Thread.startVirtualThread(() -> {
            var moreToFlush = false;
            while (moreToFlush || running.get()) {
                try {
                    if (!moreToFlush) {
                        Thread.sleep(flushPublishBufferDelay);
                    }
                    if (moreToFlush || published.getAndSet(false)) {
                        var flushed = eventRepository.flushBuffer(publishedTopics, flushPublishBufferSize);
                        moreToFlush = flushed > flushPublishBufferSize;
                    }
                } catch (Exception e) {
                    logger.error("Fail to flush publish buffer:", e);
                }
            }
        });
    }

    private void validateNewEvent(EventInput publication, TopicDefinition topic) {
        if (publication.partition() < -1) {
            throw new IllegalArgumentException("Illegal partition value: " + publication.partition());
        } else if (topic.partitions() == -1 && publication.partition() != -1) {
            throw new IllegalArgumentException("%s topic is not partitioned, but publication to %d partition was requested"
                    .formatted(topic.name(), publication.partition()));
            // partitions are numbered from 0
        } else if (topic.partitions() > -1 && (publication.partition() + 1) > topic.partitions()) {
            throw new IllegalArgumentException("%s topic has only %d partitions, but publishing to %d was requested"
                    .formatted(topic.name(), topic.partitions(), publication.partition()));
        }
    }

    private TopicDefinition findTopicDefinition(String name) {
        return topicDefinitionsCache.getLoadingIf(name)
                .orElseThrow(() -> new IllegalArgumentException("topic of %s name doesn't exist".formatted(name)));
    }

    @Override
    public void configurePartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
    }

    @Override
    public Partitioner partitioner() {
        return partitioner;
    }

    @Override
    public void stop(Duration timeout) {
        logger.info("Stopping publisher...");
        running.set(false);
        var latch = waitForPublisherToFinishAsync();
        try {
            if (latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                logger.info("Publisher has stopped gracefully!");
            } else {
                logger.warn("Publisher didn't finish in {}, exiting in any case", timeout);
            }
        } catch (Exception e) {
            logger.error("Problem while stopping publisher:", e);
        }
    }

    private CountDownLatch waitForPublisherToFinishAsync() {
        if (!flushPublishBufferThreadSet.get()) {
            return new CountDownLatch(0);
        }
        var latch = new CountDownLatch(1);
        Thread.startVirtualThread(() -> {
            while (flushPublishBufferThread.isAlive()) {
                try {
                    logger.info("Publisher is still alive, waiting for it to finish...");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            latch.countDown();
        });
        return latch;
    }
}
