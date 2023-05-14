package org.bolhoso.stream;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.net.SocketException;

@Slf4j
public class ClientConnection extends Thread {
    private Socket socket;
    private boolean isActive;
    private BufferedReader reader;

    @Getter private ClientType clientType;
    @Getter private String subscribedTopic;
    private TopicSubscriptionManager subscriptionManager;

    public ClientConnection(final Socket s, final TopicSubscriptionManager subscriptionManager) throws IOException {
        this.socket = s;
        this.subscriptionManager = subscriptionManager;
        if (this.establishConnection()) {
            this.start();
        }
    }

    @Override
    public void run() {
        if (socket == null) {
            log.error("Trying to run a handler with a null socket");
            return;
        }

        this.isActive = true;
        while (isActive && this.socket.isConnected() && !Thread.currentThread().isInterrupted()) {
            try {
                if (!readData()) {
                    log.info("Client disconnected");
                    this.socket.close();
                    this.closeSocket();
                }
            } catch (SocketException e) {
                log.info("Client connection closed", e);
                this.closeSocket();
            } catch (IOException e) {
                log.error("Error reading from socket", e);
                this.closeSocket();
            }
        }
        log.info("Terminating BrokerClientHandler");
        this.closeSocket();
    }

    private boolean establishConnection() throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // First, read the type of client
        String clientType = reader.readLine();
        this.clientType = ClientType.valueOf(clientType);

        // Now get the topic of interest
        String topic = reader.readLine();
        this.subscribedTopic = topic;
        this.subscriptionManager.addSubscription(topic, this.clientType, this);

        return true;
    }

    public void stopHandler() {
        if (!isActive || this.socket.isClosed()) {
            log.warn("Trying to stop an inactive handler");
        } else {
            try {
                this.socket.close();
            } catch (IOException e) {
                log.warn("Error closing socket", e);
            }
            this.interrupt();
        }
        this.closeSocket();
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

    public void onDataReceived(final String line) {
        this.subscriptionManager.messageReceived(this, line);
    }

    private boolean readData() throws IOException {
        String line = reader.readLine();
        while (line != null) {
            this.onDataReceived(line);
            line = reader.readLine();
        }
        return false;
    }

    private void closeSocket() {
        if (this.socket != null && this.socket.isConnected()) {
            try {
                this.socket.close();
            } catch (IOException e) {
                log.warn("Error closing socket", e);
            }
        }
        this.socket = null;
        this.isActive = false;
    }
}
