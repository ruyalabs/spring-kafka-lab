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

        /*
         * CRITICAL DESIGN CONFLICT DOCUMENTATION:
         * 
         * The current error handling configuration creates a logically impossible scenario
         * due to conflicting requirements:
         * 
         * REQUIREMENTS:
         * 1. No retries allowed (FixedBackOff(0L, 0L))
         * 2. No Dead Letter Queue (DLQ)
         * 3. No partition blocking
         * 4. Exactly one response per request (no duplicates)
         * 5. No request may go unanswered
         * 
         * THE CONFLICT:
         * If sending an error response fails (e.g., Kafka broker down), we have three options:
         * A) Retry sending the response → Violates "no retries" requirement
         * B) Send to DLQ for manual processing → Violates "no DLQ" requirement  
         * C) Block the partition until response succeeds → Violates "no blocking" requirement
         * D) Commit offset and continue → Violates "no request may go unanswered" requirement
         * 
         * CURRENT IMPLEMENTATION:
         * We chose option D (commit and continue) to prioritize system availability over
         * response guarantees. This is documented as a CRITICAL error.
         * 
         * RECOMMENDED SOLUTIONS (choose one):
         * 1. Allow limited retries in error handler with exponential backoff
         * 2. Implement persistent "failed response" storage for manual intervention
         * 3. Allow partition blocking when response sending fails
         * 4. Relax the "exactly one response" requirement to allow retries
         * 
         * BUSINESS DECISION REQUIRED: The stakeholders must choose which requirement
         * to relax, as the current combination is mathematically impossible to satisfy
         * in all failure scenarios.
         */
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (consumerRecord, exception) -> {
                    log.error("Message processing failed - Topic: {}, Partition: {}, Offset: {}, Key: {}, ErrorType: {}, ErrorMessage: {}",
                            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                            consumerRecord.key(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
                    try {
                        PaymentDto paymentDto = (PaymentDto) consumerRecord.value();
                        PaymentResponseDto errorResponse = createErrorResponse(paymentDto,
                                "Payment processing failed. Error: " + exception.getMessage());

                        nonTransactionalKafkaTemplate.send(paymentResponseTopic, paymentDto.getPaymentId(), errorResponse).get();
                        log.info("Error response sent successfully - PaymentId: {}, CustomerId: {}",
                                paymentDto.getPaymentId(), paymentDto.getCustomerId());
                    } catch (Exception e) {
                        log.error("CRITICAL BUSINESS RULE VIOLATION: Failed to send error response - " +
                                        "Topic: {}, Partition: {}, Offset: {}, Key: {}, PaymentId: {}, " +
                                        "OriginalError: {} - {}, SendError: {} - {}. " +
                                        "IMPACT: Request will go unanswered, violating business requirements. " +
                                        "ACTION REQUIRED: Manual intervention needed to send response. " +
                                        "RECOMMENDATION: Review conflicting requirements documented above.",
                                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                consumerRecord.key(), 
                                consumerRecord.value() instanceof PaymentDto ? ((PaymentDto) consumerRecord.value()).getPaymentId() : "unknown",
                                exception.getClass().getSimpleName(), exception.getMessage(),
                                e.getClass().getSimpleName(), e.getMessage(), e);

                        // TODO: Implement one of the recommended solutions above
                        // For now, we commit the offset to prevent partition blocking
                        // but this violates the "no request may go unanswered" requirement
                    }
                },
                new FixedBackOff(0L, 0L) // No retries - part of the conflicting requirements
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

        // Same conflicting requirements apply to payment execution status processing
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (consumerRecord, exception) -> {
                    log.error("Payment execution status message processing failed - Topic: {}, Partition: {}, Offset: {}, Key: {}, ErrorType: {}, ErrorMessage: {}",
                            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                            consumerRecord.key(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
                    try {
                        PaymentExecutionStatusDto statusDto = (PaymentExecutionStatusDto) consumerRecord.value();
                        PaymentResponseDto errorResponse = new PaymentResponseDto()
                                .paymentId(statusDto.getPaymentId())
                                .status(PaymentResponseDto.StatusEnum.ERROR)
                                .errorInfo("Payment execution status processing failed. Error: " + exception.getMessage());

                        nonTransactionalKafkaTemplate.send(paymentResponseTopic, statusDto.getPaymentId(), errorResponse).get();
                        log.info("Error response sent successfully for status message - PaymentId: {}",
                                statusDto.getPaymentId());
                    } catch (Exception e) {
                        log.error("CRITICAL BUSINESS RULE VIOLATION: Failed to send error response for status message - " +
                                        "Topic: {}, Partition: {}, Offset: {}, Key: {}, PaymentId: {}, " +
                                        "OriginalError: {} - {}, SendError: {} - {}. " +
                                        "IMPACT: Request will go unanswered, violating business requirements. " +
                                        "ACTION REQUIRED: Manual intervention needed to send response. " +
                                        "RECOMMENDATION: Review conflicting requirements documented above.",
                                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                consumerRecord.key(),
                                consumerRecord.value() instanceof PaymentExecutionStatusDto ? ((PaymentExecutionStatusDto) consumerRecord.value()).getPaymentId() : "unknown",
                                exception.getClass().getSimpleName(), exception.getMessage(),
                                e.getClass().getSimpleName(), e.getMessage(), e);

                        // TODO: Implement one of the recommended solutions documented above
                        // For now, we commit the offset to prevent partition blocking
                        // but this violates the "no request may go unanswered" requirement
                    }
                },
                new FixedBackOff(0L, 0L) // No retries - part of the conflicting requirements
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
