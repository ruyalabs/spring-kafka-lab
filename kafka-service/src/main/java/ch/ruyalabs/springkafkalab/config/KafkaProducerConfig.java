package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.acks:all}")
    private String acks;

    @Value("${spring.kafka.producer.retries:10}")
    private Integer retries;

    @Value("${spring.kafka.producer.properties.delivery.timeout.ms:10000}")
    private Integer deliveryTimeoutMs;

    @Value("${spring.kafka.producer.properties.request.timeout.ms:5000}")
    private Integer requestTimeoutMs;

    @Value("${spring.kafka.producer.properties.max.block.ms:5000}")
    private Integer maxBlockMs;

    @Value("${spring.kafka.producer.properties.max.in.flight.requests.per.connection:1}")
    private Integer maxInFlightRequestsPerConnection;

    @Value("${spring.kafka.producer.properties.enable.idempotence:true}")
    private Boolean enableIdempotence;

    @Value("${spring.kafka.producer.properties.retry.backoff.ms:1000}")
    private Integer retryBackoffMs;

    @Bean
    public ProducerFactory<String, PaymentDto> paymentProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, acks);
        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, PaymentDto> paymentKafkaTemplate() {
        return new KafkaTemplate<>(paymentProducerFactory());
    }
}
