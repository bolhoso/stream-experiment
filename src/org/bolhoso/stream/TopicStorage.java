package org.bolhoso.stream;

import lombok.Getter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicStorage {
    record Message(long offset, String payload, long timestamp) {}

    private List<Message> messages;
    @Getter
    private String topicName;
    private AtomicInteger offset;

    public TopicStorage(final String topicName) {
        this.topicName = topicName;

        this.offset = new AtomicInteger(0);
        this.messages = new LinkedList<>();
    }

    public synchronized void addMessage(final String payload) {
        int newOffset = this.offset.incrementAndGet();
        this.messages.add(new Message(newOffset, payload, System.currentTimeMillis()));
    }

    public String getMessage(final int pos) {
        if (pos < offset.get()) {
            return this.messages.get(pos).payload();
        }

        return null;
    }
}
