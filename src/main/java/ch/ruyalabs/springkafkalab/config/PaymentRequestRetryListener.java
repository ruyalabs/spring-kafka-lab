package ch.ruyalabs.springkafkalab.config;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.exception.InsufficientBalanceException;
import ch.ruyalabs.springkafkalab.exception.InvalidPaymentMethodException;
import ch.ruyalabs.springkafkalab.exception.PaymentProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
public class PaymentRequestRetryListener implements RetryListener {

    private static final Set<Class<? extends Exception>> NON_RETRYABLE_EXCEPTIONS = Set.of(
            InsufficientBalanceException.class,
            AccountNotFoundException.class,
            PaymentProcessingException.class,
            InvalidPaymentMethodException.class
    );

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
        String exceptionType = ex.getClass().getSimpleName();
        boolean isRetryable = !isNonRetryableException(ex);
        String retryableStatus = isRetryable ? "RETRYABLE" : "NON_RETRYABLE";
        
        // Extract payment information if available
        String paymentInfo = extractPaymentInfo(record);
        
        if (isRetryable) {
            log.warn("Retrying message for the {} time - Operation: retry_attempt, Topic: {}, Partition: {}, Offset: {}, Key: {}, RetryCount: {}, ExceptionType: {} ({}), ExceptionMessage: {}, {}",
                    getOrdinalNumber(deliveryAttempt), record.topic(), record.partition(), record.offset(), record.key(),
                    deliveryAttempt, exceptionType, retryableStatus, ex.getMessage(), paymentInfo);
        } else {
            log.error("A non-retryable exception was caught - Operation: non_retryable_exception, Topic: {}, Partition: {}, Offset: {}, Key: {}, ExceptionType: {} ({}), ExceptionMessage: {}, {}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    exceptionType, retryableStatus, ex.getMessage(), paymentInfo);
        }
    }

    @Override
    public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
        String paymentInfo = extractPaymentInfo(record);
        log.info("Message processing recovered after retry - Operation: retry_recovery, Topic: {}, Partition: {}, Offset: {}, Key: {}, {}",
                record.topic(), record.partition(), record.offset(), record.key(), paymentInfo);
    }

    @Override
    public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
        String paymentInfo = extractPaymentInfo(record);
        log.error("Message recovery failed after all retry attempts exhausted - Operation: retry_recovery_failed, Topic: {}, Partition: {}, Offset: {}, Key: {}, OriginalExceptionType: {}, OriginalExceptionMessage: {}, RecoveryFailureType: {}, RecoveryFailureMessage: {}, {}",
                record.topic(), record.partition(), record.offset(), record.key(),
                original.getClass().getSimpleName(), original.getMessage(),
                failure.getClass().getSimpleName(), failure.getMessage(), paymentInfo);
    }

    private boolean isNonRetryableException(Exception ex) {
        return NON_RETRYABLE_EXCEPTIONS.stream()
                .anyMatch(nonRetryableClass -> nonRetryableClass.isAssignableFrom(ex.getClass()));
    }

    private String extractPaymentInfo(ConsumerRecord<?, ?> record) {
        try {
            if (record.value() instanceof PaymentDto paymentDto) {
                return String.format("PaymentId: %s, CustomerId: %s, Amount: %s %s",
                        paymentDto.getPaymentId(), paymentDto.getCustomerId(),
                        paymentDto.getAmount(), paymentDto.getCurrency());
            }
        } catch (Exception e) {
            log.debug("Could not extract payment information from record: {}", e.getMessage());
        }
        return "PaymentInfo: unavailable";
    }

    private String getOrdinalNumber(int number) {
        if (number >= 11 && number <= 13) {
            return number + "th";
        }
        return switch (number % 10) {
            case 1 -> number + "st";
            case 2 -> number + "nd";
            case 3 -> number + "rd";
            default -> number + "th";
        };
    }
}