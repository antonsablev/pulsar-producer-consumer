package org.example.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Client {
    private static final Logger logger = LogManager.getLogger(Client.class);
    private static final String ENTER_REQUEST_TEXT = "Enter your request: ";
    private static final String LOGGER_SEND_REQUEST_TEXT = "Sent request from client: ";
    private static final String LOGGER_RECEIVED_REQUEST_TEXT = "Received request from server: ";
    private static final String RECEIVED_MSG_TEXT = "Received message: ";
    private static final String PRODUCER_TOPIC_NAME = "my-topic-new";
    private static final String CONSUMER_SUBSCRIPTION_NAME = "periodic-message";
    private static final String CONSUMER_TOPIC_NAME = "response-topic-new";
    private static final String URL = "pulsar://localhost:6650";
    private final PulsarClient client;
    private final Producer<String> producer;
    private final Consumer<byte[]> consumer;

    public Client() {
        this.client = runClient();
        this.producer = createProducer();
        this.consumer = createConsumer();
    }

    public void run() {
        while (true) {
            try {
                Scanner scanner = new Scanner(System.in);
                System.out.print(ENTER_REQUEST_TEXT);
                String request = scanner.nextLine();
                producer.send(request);
                logger.info(LOGGER_SEND_REQUEST_TEXT + request);
                Message<byte[]> message = consumer.receive();
                String messageString = new String(message.getData(), StandardCharsets.UTF_8);
                logger.info(LOGGER_RECEIVED_REQUEST_TEXT + messageString);
                System.out.println(RECEIVED_MSG_TEXT + messageString);
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                throw new RuntimeException("Can't send request", e);
            }
        }
    }

    private Producer<String> createProducer() {
        try {
            return client.newProducer(Schema.STRING)
                    .topic(PRODUCER_TOPIC_NAME)
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Can't create producer", e);
        }
    }

    private Consumer<byte[]> createConsumer() {
        try {
            return client.newConsumer()
                    .subscriptionName(CONSUMER_SUBSCRIPTION_NAME)
                    .topic(CONSUMER_TOPIC_NAME)
                    .subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Can't create consumer", e);
        }
    }

    private PulsarClient runClient() {
        try {
            return PulsarClient.builder()
                    .serviceUrl(URL)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Can't run client", e);
        }
    }
}
