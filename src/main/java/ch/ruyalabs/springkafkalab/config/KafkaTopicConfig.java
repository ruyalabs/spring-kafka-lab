package ch.ruyalabs.springkafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic paymentRequestTopic() {
        return new NewTopic("payment-request", 3, (short) 3);
    }
}