package org.bolhoso.stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicSubscriptionManager {
    private Map<ClientConnection, String> topicProducers;
    private Map<String, List<ClientConnection>> topicConsumers;

    public TopicSubscriptionManager() {
        this.topicConsumers = new HashMap<>();
        this.topicProducers = new HashMap<>();
    }

    public void messageReceived(final ClientConnection producer, final String message) {
        if (producer.getClientType() != ClientType.PRODUCER) {
            return;
        }
        // TODO: handle message
        System.out.printf("Thread=%s Message received: %s\n", Thread.currentThread().getName(), message);

        String topic = this.topicProducers.get(producer);
        if (topic == null) {
            throw new IllegalStateException("Producer not registered");
        }

        List<ClientConnection> consumers = this.topicConsumers.get(topic);
        for (ClientConnection consumer : consumers) {
            consumer.sendMessage(message);
        }
    }

    public void addSubscription(final String topic, final ClientType clientType, final ClientConnection clientConnection) {
        if (clientType == ClientType.CONSUMER) {
            this.topicConsumers.computeIfAbsent(topic, k -> new ArrayList<>()).add(clientConnection);
        } else if (clientType == ClientType.PRODUCER) {
            this.topicProducers.put(clientConnection, topic);
        } else {
            throw new IllegalArgumentException("Invalid client type");
        }
    }
}
