package org.bolhoso.stream;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;

@Slf4j
public class Main {
    public static void main(String[] args) throws IOException {
        BrokerServer brokerServer = new BrokerServer();
        brokerServer.startProducerServer();

        createProducer("topic1");
        createConsumer("topic1");
        createConsumer("topic1");

        createProducer("topic2");
        createConsumer("topic2");


        System.in.read();
        brokerServer.stop();

        // TODO: create producer
        // TODO: create consumer
        // TODO: create stream
    }

    public static void createProducer(final String topic) throws IOException {
        Socket socket = new Socket("localhost", BrokerServer.DEFAULT_PORT);
        Thread t = new Thread(() -> {
            try {
                Random r = new Random();
                PrintWriter pw = new PrintWriter(socket.getOutputStream());

                pw.println("PRODUCER");
                pw.println(topic);
                pw.flush();
                while (socket.isConnected() && !Thread.currentThread().isInterrupted()) {
                    String msg = "msg" + r.nextInt(100);
                    log.info("Producer {} [{}] msg={}", Thread.currentThread().getId(), topic, msg);

                    pw.println(msg);
                    pw.flush();

                    Thread.sleep(r.nextInt(1000) + 1000);
                }
            } catch (IOException | InterruptedException e) {
            }
        });
        t.start();
    }

    public static void createConsumer(final String topic) throws IOException {
        Socket socket = new Socket("localhost", BrokerServer.DEFAULT_PORT);
        Thread t = new Thread(() -> {
            try {
                Random r = new Random();
                PrintWriter pw = new PrintWriter(socket.getOutputStream());
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                pw.println("CONSUMER");
                pw.println(topic);
                pw.flush();

                while (socket.isConnected() && !Thread.currentThread().isInterrupted()) {
                    log.info("Consumer {} [{}] msg={}", Thread.currentThread().getId(), topic, br.readLine());
                }
            } catch (IOException e) {
            }
        });
        t.start();
    }
}