package br.com.julioneto.kafkaconsumer;

import br.com.julioneto.kafkaconsumer.services.KafkaConsumerService;

import java.util.concurrent.ExecutionException;

public class App {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        System.out.println("Lendo mensagens...");
        KafkaConsumerService.readMessage(System.getenv("KAFKA_GROUP_ID_READER"));
    }
}
