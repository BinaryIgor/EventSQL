package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.Consumer;
import com.binaryigor.eventsql.test.IntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.binaryigor.eventsql.test.Tests.awaitAssertion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class EventSQLRegistryTest extends IntegrationTest {

    private static final String TOPIC = "some_topic";
    private static final String CONSUMER = "some_consumer";

    @Test
    void registersNewTopicIdempotently() {
        // given
        assertThat(registry.listTopics()).isEmpty();
        var topic = new TopicDefinition(TOPIC, 2);

        // when
        registry.registerTopic(topic);
        registry.registerTopic(topic);

        // then
        assertThat(registry.listTopics())
                .containsOnly(topic);
    }

    @Test
    void registersTopicChanges() {
        // given
        var topic = new TopicDefinition(TOPIC, -1);
        registry.registerTopic(topic);
        var modifiedTopic = new TopicDefinition(topic.name(), 10);

        // when
        registry.registerTopic(modifiedTopic);

        // then
        assertThat(registry.listTopics())
                .containsOnly(modifiedTopic);
    }

    @ParameterizedTest
    @ValueSource(ints = {-99, -4, 0})
    void doesNotAllowToRegisterTopicWithInvalidPartition(int partition) {
        // expect
        assertThatThrownBy(() -> registry.registerTopic(new TopicDefinition(TOPIC, partition)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(partition));
        // and topic does not exist
        assertThat(registry.listTopics())
                .isEmpty();
    }

    @Test
    void doesNotAllowToRegisterTopicChangesWithEvents() {
        // given
        var topic = new TopicDefinition(TOPIC, -1);
        registry.registerTopic(topic);
        publisher.publish(TestObjects.randomEventPublication(TOPIC));
        flushPublishBuffer(TOPIC);

        // expect
        assertRegisterThrowsHasEventsOrConsumersException(new TopicDefinition(topic.name(), 2));
        // and topic was not changed
        assertThat(registry.listTopics())
                .containsOnly(topic);
    }

    @Test
    void doesNotAllowToRegisterTopicChangesWithConsumers() {
        // given
        var topic = new TopicDefinition(TOPIC, -1);
        registry.registerTopic(topic);
        registry.registerConsumer(new ConsumerDefinition(topic.name(), CONSUMER, false));

        // expect
        assertRegisterThrowsHasEventsOrConsumersException(new TopicDefinition(topic.name(), 2));
        // and topic was not changed
        assertThat(registry.listTopics())
                .containsOnly(topic);
    }

    @Test
    void unregistersTopicDeletingAllEvents() {
        // given
        var topic = new TopicDefinition(TOPIC, -1);
        registry.registerTopic(topic);
        // and events
        publisher.publish(TestObjects.randomEventPublication(topic.name()));
        publisher.publish(TestObjects.randomEventPublication(topic.name()));
        flushPublishBuffer(TOPIC);
        assertThat(eventTableExists(topic.name()))
                .isTrue();
        assertThat(topicEventsCount(topic.name()))
                .isEqualTo(2);

        // when
        registry.unregisterTopic(topic.name());

        // then
        assertThat(registry.listTopics())
                .doesNotContain(topic);
        assertThat(eventTableExists(topic.name())).isFalse();
    }

    @Test
    void doesNotAllowToUnregisterTopicWithConsumers() {
        // given
        var topic = new TopicDefinition(TOPIC, -1);
        registry.registerTopic(topic);
        // and consumers
        registry.registerConsumer(new ConsumerDefinition(topic.name(), "consumer-1", false));
        registry.registerConsumer(new ConsumerDefinition(topic.name(), "consumer-2", false));

        // expect
        assertThatThrownBy(() -> registry.unregisterTopic(topic.name()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot unregister topic with consumers.");
        // and topic still exists
        assertThat(registry.listTopics())
                .containsExactly(topic);
    }

    @Test
    void registersConsumerOfTopicIdempotently() {
        // given
        var topic = new TopicDefinition(TOPIC, 2);
        registry.registerTopic(topic);
        assertThat(registry.listConsumers())
                .isEmpty();

        // when
        var consumer = new ConsumerDefinition(topic.name(), "consumer", false);
        registry.registerConsumer(consumer);
        registry.registerConsumer(consumer);

        // then
        assertThat(registry.listConsumers())
                .containsOnly(consumer);
    }

    @Test
    void registersChangeForConsumerWithoutState() {
        // given
        var topic = new TopicDefinition(TOPIC, 2);
        registry.registerTopic(topic);
        var consumer = new ConsumerDefinition(topic.name(), "consumer", false);
        registry.registerConsumer(consumer);

        // when
        var changedConsumer = new ConsumerDefinition(consumer.topic(), consumer.name(), true);
        registry.registerConsumer(changedConsumer);

        // then
        assertThat(registry.listConsumers())
                .containsOnly(changedConsumer);
        // and there are two consumers in the group
        assertThat(consumerRepository.allOf(topic.name()))
                .containsExactly(
                        toConsumerOfPartition(changedConsumer, 0),
                        toConsumerOfPartition(changedConsumer, 1));
    }

    @Test
    void doesNotAllowToRegisterConsumerOfNonExistingTopic() {
        // expect
        assertThatThrownBy(() -> registry.registerConsumer(new ConsumerDefinition("non-existing-topic", "customer", false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic doesn't exist");
    }

    @Test
    void doesNotAllowToRegisterChangesForConsumerWithState() {
        // given
        var topic = new TopicDefinition(TOPIC, 2);
        registry.registerTopic(topic);
        var consumer = new ConsumerDefinition(topic.name(), "consumer", true);
        registry.registerConsumer(consumer);

        // and state, because some events were consumer
        var capturedEvents = new CopyOnWriteArrayList<Event>();
        consumers.startConsumer(topic.name(), consumer.name(), capturedEvents::add, Duration.ofMillis(10));
        publisher.publish(TestObjects.randomEventPublication(topic.name()));
        publisher.publish(TestObjects.randomEventPublication(topic.name()));
        awaitAssertion(() -> assertThat(capturedEvents).hasSize(2));

        // expect
        assertThatThrownBy(() ->
                registry.registerConsumer(new ConsumerDefinition(consumer.topic(), consumer.name(), false))
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot modify consumers with state");
        // and consumer definition has not changed
        assertThat(registry.listConsumers())
                .containsOnly(consumer);
    }

    @Test
    void unregistersConsumerDeletingAllGroupMembers() {
        // given
        var topic = new TopicDefinition(TOPIC, 2);
        registry.registerTopic(topic);
        var consumer = new ConsumerDefinition(topic.name(), "consumer", true);
        registry.registerConsumer(consumer);
        assertThat(registry.listConsumers())
                .containsExactly(consumer);
        assertThat(consumerRepository.allOf(topic.name()))
                .hasSize(topic.partitions());

        // when
        registry.unregisterConsumer(consumer.topic(), consumer.name());

        // then
        assertThat(registry.listConsumers())
                .isEmpty();
        assertThat(consumerRepository.allOf(topic.name()))
                .isEmpty();
    }

    private void assertRegisterThrowsHasEventsOrConsumersException(TopicDefinition definition) {
        assertThatThrownBy(() -> registry.registerTopic(definition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(definition.name() + " topic has events or consumers");
    }

    private int topicEventsCount(String topic) {
        return publishedEvents(topic).size();
    }

    private Consumer toConsumerOfPartition(ConsumerDefinition definition, int partition) {
        return new Consumer(definition.topic(), definition.name(), partition, null, null, null, 0);
    }
}
