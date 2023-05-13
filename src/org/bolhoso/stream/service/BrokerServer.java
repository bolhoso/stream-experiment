package org.bolhoso.stream.service;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BrokerServer {
    public static final int DEFAULT_PORT = 9000;

    private ServerSocket serverSocket;
    private int port;

    private Thread serverThread;
    private List<BrokerProducerHandler> connectedClients;
    private boolean isRunning;
    private ProducerHandler producerHandler;

    public BrokerServer() throws IOException {
        this.isRunning = false;
        this.serverSocket = new ServerSocket(DEFAULT_PORT);
        this.port = DEFAULT_PORT;

        this.producerHandler = new ProducerHandler();
        this.connectedClients = new ArrayList<>();
    }

    public void start() {
        log.info("Starting BrokerServer at port {}", this.port);
        this.isRunning = true;

        this.serverThread = new Thread(() -> {
            while (isRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    Socket socket = serverSocket.accept();
                    log.info("New connection from {}", socket.getInetAddress());

                    BrokerProducerHandler handler = new BrokerProducerHandler(socket, producerHandler);
                    handler.start();
                    this.connectedClients.add(handler);
                } catch (SocketException e) {
                    log.info("Server socket closed");
                } catch (IOException e) {
                    log.error("IOException on server", e);
                }
            }
        });
        serverThread.start();
    }

    public void stop() {
        try {
            this.serverThread.interrupt();
            this.connectedClients.forEach(BrokerProducerHandler::stopHandler);
            this.serverSocket.close();
        } catch (IOException e) {
            log.error("Error closing server socket", e);
        }
    }
}
