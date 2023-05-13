package org.bolhoso.stream;

import lombok.extern.slf4j.Slf4j;
import org.bolhoso.stream.service.BrokerServer;

import java.io.IOException;

@Slf4j
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        BrokerServer brokerServer = new BrokerServer();
        brokerServer.start();

        // TODO: create producer
        // TODO: create consumer
        // TODO: create stream
    }
}