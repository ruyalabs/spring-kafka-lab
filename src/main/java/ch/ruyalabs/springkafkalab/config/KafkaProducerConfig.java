package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.dto.PaymentResponseDto;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.producer.acks}")
    private String acks;

    @Value("${app.kafka.producer.retries}")
    private int retries;

    @Value("${app.kafka.producer.enable-idempotence}")
    private boolean enableIdempotence;

    @Value("${app.kafka.producer.transactional-id}")
    private String transactionalId;

    @Bean
    public ProducerFactory<String, PaymentResponseDto> paymentResponseProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, acks);
        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        // Enable transactions
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, PaymentResponseDto> paymentResponseKafkaTemplate() {
        KafkaTemplate<String, PaymentResponseDto> template = new KafkaTemplate<>(paymentResponseProducerFactory());
        return template;
    }

    /**
     * Non-transactional producer factory for error recovery scenarios.
     * This producer is used when the main transactional producer fails and we need to send
     * error responses or recovery messages. Since error handling occurs outside the main
     * transaction context, we need a separate non-transactional producer to avoid
     * transaction-related issues during error recovery.
     */
    @Bean
    public ProducerFactory<String, PaymentResponseDto> nonTransactionalPaymentResponseProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, acks);
        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        // No transactional ID for error recovery scenarios

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Non-transactional Kafka template for error recovery scenarios.
     * This template uses the non-transactional producer factory and is specifically
     * designed for sending error responses and recovery messages when the main
     * transactional processing fails.
     */
    @Bean
    public KafkaTemplate<String, PaymentResponseDto> nonTransactionalPaymentResponseKafkaTemplate() {
        return new KafkaTemplate<>(nonTransactionalPaymentResponseProducerFactory());
    }

    @Bean
    public KafkaTransactionManager kafkaTransactionManager() {
        return new KafkaTransactionManager(paymentResponseProducerFactory());
    }

}
