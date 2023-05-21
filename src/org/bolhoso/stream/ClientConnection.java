package org.bolhoso.stream;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;
import java.util.StringTokenizer;
import java.util.UUID;

@Slf4j
public class ClientConnection extends Thread {
    private static final long OFFSET_START = 0L;

    private final TopicSubscriptionManager subscriptionManager;
    private final MessageHandler messageHandler;

    private Socket socket;
    private BufferedReader reader;

    @Getter private ClientType clientType;

    private PrintWriter writer;
    private String clientId;

    public ClientConnection(final Socket s, final TopicSubscriptionManager subscriptionManager, final MessageHandler messageHandler) {
        this.socket = s;
        this.subscriptionManager = subscriptionManager;
        this.messageHandler = messageHandler;
        this.start();
    }

    @Override
    public void run() {
        if (socket == null) {
            log.error("Trying to run a handler with a null socket");
            return;
        }

        initializeConnection();
        while (this.socket != null && this.socket.isConnected() && !Thread.currentThread().isInterrupted()) {
            String dataRead = this.readData();
            if (dataRead == null) {
                log.info("Client disconnected");
                this.closeConnection();
                break;
            }

            try {
                handleMessage(dataRead);
            } catch (ClientNotRegisteredException e) {
                this.sendProtocolError("Client not registered");
            }
        }
        log.info("Terminating BrokerClientHandler");
        this.closeConnection();
    }

    private void initializeConnection() {
        try {
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.writer = new PrintWriter(socket.getOutputStream());
        } catch (IOException e) {
            log.error("Error creating reader/writer", e);
            this.closeConnection();
        }
    }

    private void handleMessage(final String msg) throws ClientNotRegisteredException {
        if (!msg.startsWith("/")) {
            this.sendProtocolError("Invalid command");
        }

        StringTokenizer tokenizer = new StringTokenizer(msg, " ");
        String command = tokenizer.nextToken();
        switch(command) {
            case "/hello":
                this.clientType = ClientType.valueOf(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens()) {
                    this.clientId = tokenizer.nextToken();
                } else {
                    this.clientId = UUID.randomUUID().toString().substring(0, 8);
                }
                this.subscriptionManager.registerClient(clientType, clientId);
                break;

            case "/topic":
                checkClientRegistered();

                long offset = OFFSET_START;
                String topic = tokenizer.nextToken();
                if (tokenizer.hasMoreTokens()) {
                    offset = Long.parseLong(tokenizer.nextToken());
                }
                this.subscriptionManager.addSubscription(this.clientId, topic, offset, this.clientType, this);
                break;

            case "/send":
                checkClientRegistered(ClientType.PRODUCER);

                int payloadOffset = msg.lastIndexOf(' ');
                this.messageHandler.receiveFromProducer(msg.substring(payloadOffset));
                break;

            case "/receive":
                checkClientRegistered(ClientType.CONSUMER);

                this.messageHandler.retrieveMessage();
                break;

            case "/close":
                this.closeConnection();
                break;

            default:
                this.sendProtocolError("Invalid command");
        }
    }

    private void sendProtocolError(String message) {
        this.writer.println("ERROR " + message);
        this.writer.flush();
    }

    private void checkClientRegistered(final ClientType clientType) throws ClientNotRegisteredException {
        boolean isCorrectClientType = clientType == null || this.clientType == clientType;
        if (!subscriptionManager.isClientRegistered(this.clientId) && isCorrectClientType) {
            throw new ClientNotRegisteredException();
        }
    }

    private void checkClientRegistered() throws ClientNotRegisteredException {
        checkClientRegistered(null);
    }

    public boolean sendMessage(final String data) {
        try {
            if (this.socket == null || this.socket.isClosed()) {
                log.warn("Trying to send data to an inactive socket");
                return false;
            }

            PrintStream ps = new PrintStream(this.socket.getOutputStream());
            ps.append(data + "\n");
            ps.flush();
        } catch (IOException e) {
            log.error("Error sending data", e);
            return false;
        }
        return true;
    }

    private String readData() {
        String payload = null;
        try {
            payload = reader.readLine();
        } catch (IOException e) {
            log.info("Client connection closed", e);
            this.closeConnection();
        }

        return payload;
    }

    public void closeConnection() {
        if (this.socket != null && this.socket.isConnected()) {
            try {
                this.socket.close();
            } catch (IOException e) {
                log.warn("Error closing socket", e);
            }
        }
        if (this.reader != null) {
            try {
                this.reader.close();
            } catch (IOException e) {
            }
        }

        if (this.writer != null) {
            this.writer.close();
        }
        this.socket = null;
    }
}
