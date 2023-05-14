package org.bolhoso.stream;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        BrokerServer brokerServer = new BrokerServer();
        brokerServer.startProducerServer();

        System.in.read();
        brokerServer.stop();

        // TODO: create producer
        // TODO: create consumer
        // TODO: create stream
    }
}