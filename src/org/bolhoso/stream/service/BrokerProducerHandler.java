package org.bolhoso.stream.service;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;

@Slf4j
public class BrokerProducerHandler extends Thread {
    private Socket socket;
    private boolean isActive;
    private MessageHandler producerHandler;

    public BrokerProducerHandler(final Socket s, final MessageHandler handler) {
        this.socket = s;
        this.isActive = true;
        this.producerHandler = handler;
    }

    @Override
    public void run() {
        if (socket == null) {
            log.error("Trying to run a handler with a null socket");
            cleanUp();
            return;
        }

        while (isActive && this.socket.isConnected() && !Thread.currentThread().isInterrupted()) {
            try {
                if (!readData()) {
                    log.info("Client disconnected");
                    this.socket.close();
                    this.cleanUp();
                }
            } catch (SocketException e) {
                log.info("Client connection closed", e);
                this.cleanUp();
            } catch (IOException e) {
                log.error("Error reading from socket", e);
                this.cleanUp();
            }
        }
        log.info("Terminating BrokerClientHandler");
        this.cleanUp();
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
        this.cleanUp();
    }

    private boolean readData() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line = reader.readLine();
        while (line != null) {
            this.producerHandler.handleMessage(line);
            line = reader.readLine();
        }
        return false;
    }

    private void cleanUp() {
        this.socket = null;
        this.isActive = false;
    }
}
