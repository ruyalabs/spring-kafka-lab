package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.consumer.PaymentResponseProducer;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.exception.InsufficientBalanceException;
import ch.ruyalabs.springkafkalab.exception.InvalidPaymentMethodException;
import ch.ruyalabs.springkafkalab.exception.PaymentProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.ExponentialBackOff;


@Slf4j
@Component
public class PaymentRequestErrorHandler extends DefaultErrorHandler {

    public PaymentRequestErrorHandler(PaymentResponseProducer paymentResponseProducer,
                                      @Value("${app.kafka.error-handler.retry.initial-interval}") long initialInterval,
                                      @Value("${app.kafka.error-handler.retry.multiplier}") double multiplier,
                                      @Value("${app.kafka.error-handler.retry.max-interval}") long maxInterval,
                                      @Value("${app.kafka.error-handler.retry.max-elapsed-time}") long maxElapsedTime) {
        super(new PaymentRequestRecoverer(paymentResponseProducer), createExponentialBackOff(initialInterval, multiplier, maxInterval, maxElapsedTime));

        // Configure business exceptions to not be retried
        addNotRetryableExceptions(
                InsufficientBalanceException.class,
                AccountNotFoundException.class,
                PaymentProcessingException.class,
                InvalidPaymentMethodException.class
        );
    }

    private static ExponentialBackOff createExponentialBackOff(long initialInterval, double multiplier, long maxInterval, long maxElapsedTime) {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);
        backOff.setMaxElapsedTime(maxElapsedTime);
        return backOff;
    }

    @Override
    public void handleRemaining(Exception thrownException,
                                java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>> records,
                                Consumer<?, ?> consumer,
                                org.springframework.kafka.listener.MessageListenerContainer container) {

        // Set MDC context for structured logging
        MDC.put("operation", "error_handling");
        MDC.put("errorHandlerType", "handleRemaining");
        MDC.put("recordCount", String.valueOf(records.size()));

        try {
            log.error("Error handler processing remaining records after retries exhausted",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "error_handler_final_failure"),
                    net.logstash.logback.argument.StructuredArguments.kv("recordCount", records.size()),
                    net.logstash.logback.argument.StructuredArguments.kv("exceptionType", thrownException.getClass().getSimpleName()),
                    net.logstash.logback.argument.StructuredArguments.kv("exceptionMessage", thrownException.getMessage()));

            for (org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record : records) {
                logRetryAttempt(record, thrownException, "FINAL_FAILURE");
            }

            super.handleRemaining(thrownException, records, consumer, container);
        } finally {
            MDC.clear();
        }
    }

    @Override
    public boolean handleOne(Exception thrownException,
                             org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                             Consumer<?, ?> consumer,
                             org.springframework.kafka.listener.MessageListenerContainer container) {

        logRetryAttempt(record, thrownException, "RETRY_ATTEMPT");

        return super.handleOne(thrownException, record, consumer, container);
    }

    private void logRetryAttempt(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                                 Exception exception, String attemptType) {
        // Set MDC context for structured logging
        MDC.put("operation", "error_handling");
        MDC.put("attemptType", attemptType);
        MDC.put("topic", record.topic());
        MDC.put("partition", String.valueOf(record.partition()));
        MDC.put("offset", String.valueOf(record.offset()));

        try {
            log.error("Error handler processing record retry attempt",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "error_handler_retry"),
                    net.logstash.logback.argument.StructuredArguments.kv("attemptType", attemptType),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", record.topic()),
                    net.logstash.logback.argument.StructuredArguments.kv("partition", record.partition()),
                    net.logstash.logback.argument.StructuredArguments.kv("offset", record.offset()),
                    net.logstash.logback.argument.StructuredArguments.kv("key", record.key()),
                    net.logstash.logback.argument.StructuredArguments.kv("exceptionType", exception.getClass().getSimpleName()),
                    net.logstash.logback.argument.StructuredArguments.kv("exceptionMessage", exception.getMessage()));
        } finally {
            MDC.clear();
        }
    }


    @RequiredArgsConstructor
    private static class PaymentRequestRecoverer implements ConsumerRecordRecoverer {

        private final PaymentResponseProducer paymentResponseProducer;

        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception exception) {
            // Set MDC context for structured logging
            MDC.put("operation", "record_recovery");
            MDC.put("topic", record.topic());
            MDC.put("partition", String.valueOf(record.partition()));
            MDC.put("offset", String.valueOf(record.offset()));

            try {
                log.error("Record recoverer processing failed record after all retries exhausted",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "record_recovery_start"),
                        net.logstash.logback.argument.StructuredArguments.kv("topic", record.topic()),
                        net.logstash.logback.argument.StructuredArguments.kv("partition", record.partition()),
                        net.logstash.logback.argument.StructuredArguments.kv("offset", record.offset()),
                        net.logstash.logback.argument.StructuredArguments.kv("key", record.key()),
                        net.logstash.logback.argument.StructuredArguments.kv("exceptionType", exception.getClass().getSimpleName()),
                        net.logstash.logback.argument.StructuredArguments.kv("exceptionMessage", exception.getMessage()));

                // Try to extract PaymentDto from the record
                PaymentDto paymentDto = extractPaymentDto(record, exception);

                if (paymentDto != null) {
                    MDC.put("paymentId", paymentDto.getPaymentId());
                    MDC.put("customerId", paymentDto.getCustomerId());

                    String errorMessage = buildErrorMessage(exception);
                    paymentResponseProducer.sendErrorResponseNonTransactional(paymentDto, errorMessage);
                    log.info("Error response sent successfully for failed payment",
                            net.logstash.logback.argument.StructuredArguments.kv("event", "error_response_sent"),
                            net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentDto.getPaymentId()),
                            net.logstash.logback.argument.StructuredArguments.kv("customerId", paymentDto.getCustomerId()));
                } else {
                    log.error("Could not extract PaymentDto from failed record",
                            net.logstash.logback.argument.StructuredArguments.kv("event", "payment_dto_extraction_failed"),
                            net.logstash.logback.argument.StructuredArguments.kv("reason", "unable_to_extract_payment_dto"));
                }
            } catch (Exception e) {
                log.error("Exception occurred while sending error response in recoverer",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "error_response_send_failed"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", e.getClass().getSimpleName()),
                        net.logstash.logback.argument.StructuredArguments.kv("errorMessage", e.getMessage()));
            } finally {
                MDC.clear();
            }
        }

        private PaymentDto extractPaymentDto(ConsumerRecord<?, ?> record, Exception exception) {
            try {
                // If the record value is already a PaymentDto (successful deserialization)
                if (record.value() instanceof PaymentDto) {
                    return (PaymentDto) record.value();
                }

                // If it's a deserialization exception, we might not be able to recover the PaymentDto
                if (exception instanceof DeserializationException) {
                    log.warn("Deserialization exception occurred, cannot extract PaymentDto from record",
                            net.logstash.logback.argument.StructuredArguments.kv("event", "payment_dto_extraction_warning"),
                            net.logstash.logback.argument.StructuredArguments.kv("reason", "deserialization_exception"),
                            net.logstash.logback.argument.StructuredArguments.kv("exceptionType", exception.getClass().getSimpleName()));
                    return null;
                }

                // For other exceptions, the value should be available
                if (record.value() instanceof PaymentDto) {
                    return (PaymentDto) record.value();
                }

                log.warn("Record value is not a PaymentDto",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "payment_dto_extraction_warning"),
                        net.logstash.logback.argument.StructuredArguments.kv("reason", "invalid_record_value_type"),
                        net.logstash.logback.argument.StructuredArguments.kv("recordValueType", record.value() != null ? record.value().getClass().getSimpleName() : "null"));
                return null;

            } catch (Exception e) {
                log.error("Exception while extracting PaymentDto",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "payment_dto_extraction_error"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", e.getClass().getSimpleName()),
                        net.logstash.logback.argument.StructuredArguments.kv("errorMessage", e.getMessage()));
                return null;
            }
        }

        private String buildErrorMessage(Exception exception) {
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("Payment processing failed after all retry attempts. ");
            errorMsg.append("Error: ").append(exception.getMessage());

            if (exception.getCause() != null) {
                errorMsg.append(" Cause: ").append(exception.getCause().getMessage());
            }

            return errorMsg.toString();
        }
    }
}
