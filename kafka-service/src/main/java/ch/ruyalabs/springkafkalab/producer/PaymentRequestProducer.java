package ch.ruyalabs.springkafkalab.producer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRequestProducer {

    private final KafkaTemplate<String, PaymentDto> kafkaTemplate;

    @Value("${payment.request.topic:payment-request}")
    private String topic;

    /**
     * Sends a payment request to Kafka
     *
     * @param paymentDto the payment request to send
     * @return a CompletableFuture that will be completed when the send operation completes
     */
    public CompletableFuture<SendResult<String, PaymentDto>> sendPaymentRequest(PaymentDto paymentDto) {
        log.info("Sending payment request to topic {}: {}", topic, paymentDto);

        // Use the payment ID as the message key for partitioning and ordering
        String key = Optional.ofNullable(paymentDto.getId()).orElse(UUID.randomUUID().toString());


        // Send the message asynchronously and return the CompletableFuture
        CompletableFuture<SendResult<String, PaymentDto>> future = 
            kafkaTemplate.send(topic, key, paymentDto);

        // Add callbacks for success and failure handling
        future.whenComplete(createCallbacks(paymentDto));

        return future;
    }

    /**
     * Creates callbacks for handling send success and failure
     *
     * @param paymentDto the payment request being sent
     * @return a BiConsumer that handles the result of the send operation
     */
    private BiConsumer<SendResult<String, PaymentDto>, Throwable> createCallbacks(PaymentDto paymentDto) {
        return (result, exception) -> {
            if (exception != null) {
                handleFailure(paymentDto, exception);
            } else {
                handleSuccess(paymentDto, result);
            }
        };
    }

    /**
     * Handles successful message delivery
     *
     * @param paymentDto the payment request that was sent
     * @param result the result of the send operation
     */
    private void handleSuccess(PaymentDto paymentDto, SendResult<String, PaymentDto> result) {
        log.info("Payment request successfully delivered to topic {}, partition {}, offset {}",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }

    /**
     * Handles failed message delivery
     *
     * @param paymentDto the payment request that failed to send
     * @param exception the exception that occurred
     */
    private void handleFailure(PaymentDto paymentDto, Throwable exception) {
        log.error("Failed to deliver payment request to Kafka: {}", paymentDto, exception);
        // The KafkaTemplate will handle retries based on the configuration
        // We just need to log the failure here
    }
}
