package ch.ruyalabs.springkafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic paymentRequestTopic() {
        return TopicBuilder.name("payment-request")
                .partitions(3) // Desired number of partitions
                .replicas(3)   // Desired replication factor (matches your number of brokers)
                .build();
    }


    @Bean
    public NewTopic paymentResponseTopic() {
        return TopicBuilder.name("payment-response")
                .partitions(3) // Desired number of partitions
                .replicas(3)   // Desired replication factor (matches your number of brokers)
                .build();
    }

}