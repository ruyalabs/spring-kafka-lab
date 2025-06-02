package ch.ruyalabs.springkafkalab.errorhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

/**
 * Custom error handler for payment request consumer.
 * Implements retry logic for REST service errors without using a DLQ.
 */
@Slf4j
@Component
public class PaymentRequestErrorHandler extends DefaultErrorHandler {

    /**
     * Constructor with custom configuration for retries.
     * 
     * @param initialInterval initial backoff interval in ms
     * @param multiplier backoff multiplier
     * @param maxInterval maximum backoff interval in ms
     */
    public PaymentRequestErrorHandler(
            @Value("${kafka.consumer.retry.initial-interval:1000}") long initialInterval,
            @Value("${kafka.consumer.retry.multiplier:2.0}") double multiplier,
            @Value("${kafka.consumer.retry.max-interval:60000}") long maxInterval) {

        // Create exponential backoff strategy
        super(createBackOff(initialInterval, multiplier, maxInterval));

        // Configure which exceptions should be retried
        // REST client exceptions should be retried
        // Also include RuntimeException for business exceptions like "Insufficient Funds"
        addRetryableExceptions(
            HttpClientErrorException.class,
            HttpServerErrorException.class,
            ResourceAccessException.class,
            RuntimeException.class
        );

        // Add logging for retries
        super.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.error("[Retry {}] Failed to process message: key={}, value={}, exception={}",
                    deliveryAttempt, record.key(), record.value(), ex.getMessage(), ex);
        });
    }

    /**
     * Creates an exponential backoff strategy for retries.
     */
    private static BackOff createBackOff(long initialInterval, double multiplier, long maxInterval) {
        ExponentialBackOff backOff = new ExponentialBackOff(initialInterval, multiplier);
        backOff.setMaxInterval(maxInterval);
        // No max attempts - retry indefinitely for REST service errors
        return backOff;
    }
}
