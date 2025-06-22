package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.consumer.payment-request.group-id}")
    private String paymentRequestGroupId;

    @Value("${app.kafka.consumer.payment-request.trusted-packages}")
    private String trustedPackages;

    @Value("${app.kafka.consumer.payment-request.auto-offset-reset}")
    private String autoOffsetReset;

    @Bean
    public ConsumerFactory<String, PaymentDto> paymentRequestConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, paymentRequestGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, trustedPackages);
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentDto.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentDto> paymentRequestKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PaymentDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentRequestConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(new org.springframework.kafka.listener.DefaultErrorHandler());
        return factory;
    }
}
