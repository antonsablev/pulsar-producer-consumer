package org.example.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.*;
import org.example.server.handler.ClientRequestHandler;

import java.nio.charset.StandardCharsets;

public class Server {
    private static final Logger logger = LogManager.getLogger(RunPulsarServer.class);
    private static final String LOGGER_SEND_REQUEST_TEXT = "Received request from client: ";
    private static final String LOGGER_RECEIVED_REQUEST_TEXT = "Send response to client: ";
    private static final String RECEIVED_MSG_TEXT = "Received message: ";
    private static final String PRODUCER_TOPIC_NAME = "response-topic-new";
    private static final String CONSUMER_SUBSCRIPTION_NAME = "my-subscription";
    private static final String CONSUMER_TOPIC_NAME = "my-topic-new";
    private static final String URL = "pulsar://localhost:6650";
    private final ClientRequestHandler requestHandler;
    private final PulsarClient client;
    private final Producer<String> responseProducer;
    private final Consumer<byte[]> consumer;

    public Server() {
        this.requestHandler = new ClientRequestHandler();
        this.client = runClient();
        this.responseProducer = createProducer(client);
        this.consumer = createConsumer(client);
    }

    public void run() {
        while (true) {
            Message<byte[]> msg = null;
            try {
                msg = consumer.receive();
                String request = new String(msg.getData(), StandardCharsets.UTF_8);
                System.out.println(RECEIVED_MSG_TEXT + request);
                logger.info(LOGGER_SEND_REQUEST_TEXT + request);
                consumer.acknowledge(msg);
                String response = requestHandler.processInput(request);
                responseProducer.send(response);
                logger.info(LOGGER_RECEIVED_REQUEST_TEXT + response);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Consumer<byte[]> createConsumer(PulsarClient client) {
        try {
            return client.newConsumer()
                    .subscriptionName(CONSUMER_SUBSCRIPTION_NAME)
                    .topic(CONSUMER_TOPIC_NAME)
                    .subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Can't create consumer!", e);
        }
    }

    private Producer<String> createProducer(PulsarClient client) {
        try {
            return client.newProducer(Schema.STRING)
                    .topic(PRODUCER_TOPIC_NAME)
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Can't create producer!", e);
        }
    }

    private PulsarClient runClient(){
        try {
            return PulsarClient.builder()
                    .serviceUrl(URL)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Can't create client", e);
        }
    }
}
