package ch.ruyalabs.springkafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.payment-request}")
    private String paymentRequestTopicName;

    @Value("${app.kafka.topics.payment-response}")
    private String paymentResponseTopicName;

    @Value("${app.kafka.topics.payment-execution-status}")
    private String paymentExecutionStatusTopicName;

    @Value("${app.kafka.topics.partitions}")
    private int partitions;

    @Value("${app.kafka.topics.replication-factor}")
    private short replicationFactor;

    @Bean
    public NewTopic paymentRequestTopic() {
        return new NewTopic(paymentRequestTopicName, partitions, replicationFactor);
    }

    @Bean
    public NewTopic paymentResponseTopic() {
        return new NewTopic(paymentResponseTopicName, partitions, replicationFactor);
    }

    @Bean
    public NewTopic paymentExecutionStatusTopic() {
        return new NewTopic(paymentExecutionStatusTopicName, partitions, replicationFactor);
    }
}
