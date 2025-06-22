package ch.ruyalabs.springkafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.payment-request}")
    private String paymentRequestTopicName;

    @Bean
    public NewTopic paymentRequestTopic() {
        return new NewTopic(paymentRequestTopicName, 3, (short) 3);
    }
}
