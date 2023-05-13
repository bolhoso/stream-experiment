package org.bolhoso.stream.service;

public class ProducerHandler implements MessageHandler {

    @Override
    public void handleMessage(String message) {
        // TODO: handle message
        System.out.printf("Thread=%s Message received: %s\n", Thread.currentThread().getName(), message);
    }
}
