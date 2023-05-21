package org.bolhoso.stream;

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

    private ServerSocket producerServerSocket;
    private Thread producerServerThread;
    private boolean isRunning;

    private List<ClientConnection> connectedProducers;
    private List<ClientConnection> connectedConsumers;

    private TopicSubscriptionManager topicSubscriptionManager;

    public BrokerServer() throws IOException {
        this.isRunning = false;
        this.producerServerSocket = new ServerSocket(DEFAULT_PORT);

        this.topicSubscriptionManager = new TopicSubscriptionManager();
        this.connectedProducers = new ArrayList<>();
        this.connectedConsumers = new ArrayList<>();
    }

    public void startProducerServer() {
        log.info("Starting ProducerServer at port {}", this.producerServerSocket.getLocalPort());
        this.isRunning = true;

        this.producerServerThread = new Thread(() -> {
            while (isRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    Socket socket = producerServerSocket.accept();
                    log.info("New connection from {}", socket.getInetAddress());

                    connectClient(socket);
                } catch (SocketException e) {
                    log.info("Server socket closed");
                } catch (IOException e) {
                    log.error("IOException on server", e);
                }
            }
        });
        producerServerThread.start();
    }

    public void stop() {
        try {
            this.producerServerThread.interrupt();
            this.connectedProducers.forEach(ClientConnection::closeConnection);
            this.producerServerSocket.close();
        } catch (IOException e) {
            log.error("Error closing producer server socket", e);
        }
    }

    private void connectClient(final Socket socket) throws IOException {
        ClientConnection client = new ClientConnection(socket, this.topicSubscriptionManager);
        ClientType type = client.getClientType();
        switch (type) {
            case PRODUCER:
                this.connectedProducers.add(client);
                break;
            case CONSUMER:
                this.connectedConsumers.add(client);
                break;
            default:
                throw new IllegalArgumentException("Unsupported client type informed");
        }
    }
}
