package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.dto.PaymentExecutionStatusDto;
import ch.ruyalabs.springkafkalab.dto.PaymentResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final KafkaTemplate<String, PaymentResponseDto> nonTransactionalKafkaTemplate;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties,
                               @Qualifier("nonTransactionalKafkaTemplate") KafkaTemplate<String, PaymentResponseDto> nonTransactionalKafkaTemplate) {
        this.kafkaProperties = kafkaProperties;
        this.nonTransactionalKafkaTemplate = nonTransactionalKafkaTemplate;
    }

    @Value("${app.kafka.consumer.payment-request.group-id}")
    private String paymentRequestGroupId;

    @Value("${app.kafka.consumer.payment-request.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${app.kafka.consumer.payment-request.enable-auto-commit}")
    private boolean enableAutoCommit;

    @Value("${app.kafka.topics.payment-response}")
    private String paymentResponseTopic;

    @Bean
    public ConsumerFactory<String, PaymentDto> paymentRequestConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>(kafkaProperties.buildConsumerProperties());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, paymentRequestGroupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentDto> paymentRequestKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PaymentDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentRequestConsumerFactory());
        factory.setConcurrency(1);
        factory.setBatchListener(false);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (consumerRecord, exception) -> {
                    log.error("Message processing failed - Topic: {}, Partition: {}, Offset: {}, Key: {}, ErrorType: {}, ErrorMessage: {}",
                            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                            consumerRecord.key(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
                    try {
                        PaymentDto paymentDto = (PaymentDto) consumerRecord.value();
                        PaymentResponseDto errorResponse = createErrorResponse(paymentDto,
                                "Payment processing failed. Error: " + exception.getMessage());

                        nonTransactionalKafkaTemplate.send(paymentResponseTopic, paymentDto.getPaymentId(), errorResponse);
                        log.info("Error response sent successfully - PaymentId: {}, CustomerId: {}",
                                paymentDto.getPaymentId(), paymentDto.getCustomerId());
                    } catch (Exception e) {
                        log.error("CRITICAL: Failed to send error response - Topic: {}, Partition: {}, Offset: {}, Key: {}, " +
                                        "OriginalErrorType: {}, OriginalErrorMessage: {}, SendErrorType: {}, SendErrorMessage: {}. " +
                                        "This violates the exactly-one-response guarantee and requires MANUAL INTERVENTION. " +
                                        "The poison pill message will be committed to prevent partition blocking.",
                                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                consumerRecord.key(), exception.getClass().getSimpleName(), exception.getMessage(),
                                e.getClass().getSimpleName(), e.getMessage(), e);
                        // Do not re-throw - allow offset commit to prevent infinite loop and partition blocking
                    }
                },
                new FixedBackOff(0L, 0L)
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, PaymentExecutionStatusDto> paymentExecutionStatusConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>(kafkaProperties.buildConsumerProperties());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, paymentRequestGroupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentExecutionStatusDto> paymentExecutionStatusKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PaymentExecutionStatusDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentExecutionStatusConsumerFactory());
        factory.setConcurrency(1);
        factory.setBatchListener(false);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (consumerRecord, exception) -> {
                    log.error("Payment execution status message processing failed - Topic: {}, Partition: {}, Offset: {}, Key: {}, ErrorType: {}, ErrorMessage: {}",
                            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                            consumerRecord.key(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
                    try {
                        PaymentExecutionStatusDto statusDto = (PaymentExecutionStatusDto) consumerRecord.value();
                        // Create error response for the original payment request
                        PaymentResponseDto errorResponse = new PaymentResponseDto()
                                .paymentId(statusDto.getPaymentId())
                                .status(PaymentResponseDto.StatusEnum.ERROR)
                                .errorInfo("Payment execution status processing failed. Error: " + exception.getMessage());

                        nonTransactionalKafkaTemplate.send(paymentResponseTopic, statusDto.getPaymentId(), errorResponse);
                        log.info("Error response sent successfully for status message - PaymentId: {}",
                                statusDto.getPaymentId());
                    } catch (Exception e) {
                        log.error("CRITICAL: Failed to send error response for status message - Topic: {}, Partition: {}, Offset: {}, Key: {}, " +
                                        "OriginalErrorType: {}, OriginalErrorMessage: {}, SendErrorType: {}, SendErrorMessage: {}. " +
                                        "This violates the exactly-one-response guarantee and requires MANUAL INTERVENTION. " +
                                        "The poison pill message will be committed to prevent partition blocking.",
                                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                consumerRecord.key(), exception.getClass().getSimpleName(), exception.getMessage(),
                                e.getClass().getSimpleName(), e.getMessage(), e);
                        // Do not re-throw - allow offset commit to prevent infinite loop and partition blocking
                    }
                },
                new FixedBackOff(0L, 0L)
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    private PaymentResponseDto createErrorResponse(PaymentDto originalRequest, String errorMessage) {
        return new PaymentResponseDto()
                .paymentId(originalRequest.getPaymentId())
                .amount(originalRequest.getAmount())
                .currency(originalRequest.getCurrency())
                .paymentMethod(PaymentResponseDto.PaymentMethodEnum.fromValue(originalRequest.getPaymentMethod().getValue()))
                .customerId(originalRequest.getCustomerId())
                .status(PaymentResponseDto.StatusEnum.ERROR)
                .errorInfo(errorMessage);
    }
}
