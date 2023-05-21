package org.bolhoso.stream;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class TopicSubscriptionManager {
    private static final Long OFFSET_END = -1L;

    private Map<ClientConnection, String> topicProducers;
    private Map<String, List<ClientConnection>> topicConsumers;
    private Map<String, TopicStorage> topicStorage;

    public TopicSubscriptionManager() {
        this.topicConsumers = new HashMap<>();
        this.topicProducers = new HashMap<>();
        this.topicStorage = new HashMap<>();
    }

    public void addSubscription(final String clientId, final String topic, final ClientType clientType, final ClientConnection clientConnection) {
        this.addSubscription(clientId, topic, OFFSET_END, clientType, clientConnection);
    }

    public void addSubscription(final String clientId, final String topic, final Long offSet, final ClientType clientType, final ClientConnection clientConnection) {
        if (clientType == ClientType.CONSUMER) {
            this.topicConsumers.computeIfAbsent(topic, k -> new ArrayList<>()).add(clientConnection);
        } else if (clientType == ClientType.PRODUCER) {
            this.topicProducers.put(clientConnection, topic);
        } else {
            throw new IllegalArgumentException("Invalid client type");
        }

        this.topicStorage.computeIfAbsent(topic, k -> new TopicStorage(topic));
    }

    public void messageReceived(final ClientConnection producer, final String message) {
        if (producer.getClientType() != ClientType.PRODUCER) {
            return;
        }
        log.debug("[{}] Message received: {}", Thread.currentThread().getName(), message);

        String topic = this.topicProducers.get(producer);
        if (topic == null) {
            throw new IllegalStateException("Producer not registered");
        }

        List<ClientConnection> consumers = this.topicConsumers.get(topic);
        if (consumers != null) {
            for (ClientConnection consumer : consumers) {
                consumer.sendMessage(message);
            }
        }
    }

    public void registerClient(ClientType clientType, String clientId) {

    }

    public boolean isClientRegistered(String clientId) {
        return false;
    }
}
