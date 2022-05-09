package br.com.julioneto.kafkaconsumer.services;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaConsumerService {
    public static void readMessage(String groupId) throws InterruptedException, ExecutionException {
        var consumer = new KafkaConsumer<String, String>(properties(groupId));
        consumer.subscribe(Collections.singletonList(System.getenv("KAFKA_TOPIC")));

        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> registro : records) {
                if (!registro.value().equals("--exit_chat")) {
                    System.out.println("------------------------------------------");
                    System.out.println("Horário: " + registro.key());
                    System.out.println("Mensagem: " + registro.value());
                    System.out.println("Partition: " + registro.partition());
                    System.out.println("offset: " + registro.offset());
                    System.out.println("------------------------------------------");
                }
            }
        }
    }

    private static Properties properties(String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_HOST"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId); // item que identifica qual consumidor irá ler a mensagem
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); // para enviar dados em consumidores diferentes
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // para evitar conflito de partições e rebalanciamento
        return properties;
    }
}
