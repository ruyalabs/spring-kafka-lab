package ch.ruyalabs.springkafkalab.errorhandler;

import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.dto.ErrorResponseDto;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.producer.PaymentResponseProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.time.OffsetDateTime;

/**
 * Custom error handler for payment request consumer.
 * Handles all exceptions without retry logic and sends appropriate error responses.
 * All exceptions are treated as non-retryable.
 */
@Slf4j
@Component
public class PaymentRequestErrorHandler extends DefaultErrorHandler {

    private final PaymentResponseProducer paymentResponseProducer;

    public PaymentRequestErrorHandler(PaymentResponseProducer paymentResponseProducer) {
        // Configure with no retries - all exceptions are non-retryable
        super(new PaymentRequestRecoverer(paymentResponseProducer), new FixedBackOff(0L, 0L));
        this.paymentResponseProducer = paymentResponseProducer;
    }

    /**
     * Custom recoverer that handles failed records by sending error responses
     */
    private static class PaymentRequestRecoverer implements ConsumerRecordRecoverer {

        private final PaymentResponseProducer paymentResponseProducer;

        public PaymentRequestRecoverer(PaymentResponseProducer paymentResponseProducer) {
            this.paymentResponseProducer = paymentResponseProducer;
        }

        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception exception) {
            String requestId = "unknown";
            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset();

            try {
                // Try to extract request ID from the record
                if (record.value() instanceof PaymentDto) {
                    PaymentDto payment = (PaymentDto) record.value();
                    requestId = payment.getId();
                } else if (record.value() instanceof String) {
                    // Try to parse as JSON to extract ID
                    try {
                        String jsonValue = (String) record.value();
                        if (jsonValue.contains("\"id\"")) {
                            // Simple extraction of ID from JSON string
                            int idStart = jsonValue.indexOf("\"id\"") + 5;
                            int valueStart = jsonValue.indexOf("\"", idStart) + 1;
                            int valueEnd = jsonValue.indexOf("\"", valueStart);
                            if (valueEnd > valueStart) {
                                requestId = jsonValue.substring(valueStart, valueEnd);
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to extract request ID from JSON string: {}", e.getMessage());
                    }
                }

                // Classify the error and determine error code
                ErrorClassification errorClassification = classifyError(exception);

                // Log the error with structured logging
                logStructuredError(requestId, topic, partition, offset, exception, errorClassification);

                // Send error response
                paymentResponseProducer.sendErrorResponse(
                    requestId,
                    errorClassification.errorCode,
                    errorClassification.errorMessage,
                    errorClassification.isFunctional
                );

            } catch (Exception e) {
                // If everything fails, log the critical error and try to send a basic technical error response
                log.error("[DEBUG_LOG] CRITICAL: Failed to handle error for record from topic={}, partition={}, offset={}: {}", 
                         topic, partition, offset, e.getMessage(), e);

                try {
                    paymentResponseProducer.sendErrorResponse(
                        requestId,
                        ErrorResponseDto.ErrorCodeEnum.TECHNICAL_ERROR,
                        "Critical error in error handling: " + e.getMessage(),
                        false
                    );
                } catch (Exception criticalException) {
                    log.error("[DEBUG_LOG] CRITICAL: Failed to send error response after error handling failure: {}", 
                             criticalException.getMessage(), criticalException);
                }
            }
        }

        /**
         * Classifies errors into functional or technical categories
         */
        private static ErrorClassification classifyError(Exception exception) {
            if (exception instanceof IllegalArgumentException) {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.VALIDATION_FAILED,
                    "Validation failed: " + exception.getMessage(),
                    true // Functional error
                );
            } else if (exception instanceof AccountBalanceClient.AccountNotFoundException) {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.ACCOUNT_NOT_FOUND,
                    "Account not found: " + exception.getMessage(),
                    true // Functional error
                );
            } else if (exception instanceof AccountBalanceClient.InsufficientFundsException) {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.INSUFFICIENT_FUNDS,
                    "Insufficient funds: " + exception.getMessage(),
                    true // Functional error
                );
            } else if (exception instanceof AccountBalanceClient.CurrencyMismatchException) {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.CURRENCY_MISMATCH,
                    "Currency mismatch: " + exception.getMessage(),
                    true // Functional error
                );
            } else if (exception instanceof BookingClient.BookingFailedException) {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.BOOKING_FAILED,
                    "Booking failed: " + exception.getMessage(),
                    true // Functional error
                );
            } else if (exception instanceof BookingClient.BookingTimeoutException) {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.BOOKING_TIMEOUT,
                    "Booking timeout: " + exception.getMessage(),
                    true // Functional error
                );
            } else if (exception instanceof DeserializationException || 
                       exception instanceof JsonProcessingException) {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.DESERIALIZATION_ERROR,
                    "Deserialization error: " + exception.getMessage(),
                    false // Technical error
                );
            } else {
                return new ErrorClassification(
                    ErrorResponseDto.ErrorCodeEnum.TECHNICAL_ERROR,
                    "Technical error: " + exception.getMessage(),
                    false // Technical error
                );
            }
        }

        /**
         * Logs errors with structured logging including all required details
         */
        private static void logStructuredError(String requestId, String topic, int partition, long offset, 
                                      Exception exception, ErrorClassification errorClassification) {
            String errorType = errorClassification.isFunctional ? "FUNCTIONAL" : "TECHNICAL";

            log.error("[DEBUG_LOG] Payment processing failed - " +
                     "timestamp={}, requestId={}, topic={}, partition={}, offset={}, " +
                     "errorType={}, errorCode={}, errorMessage={}, exceptionClass={}", 
                     OffsetDateTime.now(),
                     requestId,
                     topic,
                     partition,
                     offset,
                     errorType,
                     errorClassification.errorCode,
                     errorClassification.errorMessage,
                     exception.getClass().getSimpleName(),
                     exception);
        }

        /**
         * Helper class to hold error classification information
         */
        private static class ErrorClassification {
            final ErrorResponseDto.ErrorCodeEnum errorCode;
            final String errorMessage;
            final boolean isFunctional;

            ErrorClassification(ErrorResponseDto.ErrorCodeEnum errorCode, String errorMessage, boolean isFunctional) {
                this.errorCode = errorCode;
                this.errorMessage = errorMessage;
                this.isFunctional = isFunctional;
            }
        }
    }
}
