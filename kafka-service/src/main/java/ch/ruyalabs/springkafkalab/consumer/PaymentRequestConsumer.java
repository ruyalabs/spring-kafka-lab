package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumer for payment requests from Kafka.
 * Processes payment requests by calling external services and acknowledges messages only after successful processing.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRequestConsumer {

    private final AccountBalanceClient accountBalanceClient;
    private final BookingClient bookingClient;

    /**
     * Processes payment requests from the payment-request topic.
     * 
     * @param payment the payment request
     * @param key the message key
     * @param partition the partition from which the message was received
     * @param topic the topic from which the message was received
     * @param offset the offset of the message
     * @param acknowledgment the acknowledgment callback
     */
    @KafkaListener(
        topics = "${payment.request.topic:payment-request}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "paymentKafkaListenerContainerFactory"
    )
    public void processPayment(
            @Payload PaymentDto payment,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) Long offset,
            Acknowledgment acknowledgment) {

        log.info("Received payment request: key={}, payment={}, topic={}, partition={}, offset={}",
                key, payment, topic, partition, offset);

        try {
            // Step 1: Check account balance
            log.info("Checking account balance for payment: {}", payment.getId());
            // Using payment ID as account ID for demonstration
            String accountId = "account-" + payment.getId();
            AccountBalanceClient.AccountBalance balance = accountBalanceClient.getBalance(accountId);
            log.info("Account balance for payment {}: {} {}", 
                    payment.getId(), balance.balance(), balance.currency());

            // Verify sufficient funds
            if (balance.balance().compareTo(payment.getAmount()) < 0) {
                log.error("Insufficient funds for payment {}: {} {} (available: {} {})",
                        payment.getId(), payment.getAmount(), payment.getCurrency(),
                        balance.balance(), balance.currency());
                throw new RuntimeException("Insufficient funds for payment: " + payment.getId());
            }

            // Step 2: Create booking
            log.info("Creating booking for payment: {}", payment.getId());
            BookingClient.BookingRequest bookingRequest = new BookingClient.BookingRequest(
                    accountId,
                    "Payment: " + payment.getDescription(),
                    1 // Quantity
            );

            BookingClient.BookingResponse bookingResponse = bookingClient.createBooking(bookingRequest);
            log.info("Booking created for payment {}: bookingId={}, status={}",
                    payment.getId(), bookingResponse.bookingId(), bookingResponse.status());

            // If we got here, both service calls were successful
            log.info("Successfully processed payment: {}", payment.getId());

            // Acknowledge the message
            acknowledgment.acknowledge();
            log.info("Acknowledged payment request: {}", payment.getId());

        } catch (Exception e) {
            // Log the error but don't acknowledge - let the error handler deal with retries
            log.error("Error processing payment request {}: {}", payment.getId(), e.getMessage(), e);
            // The message will be retried by the error handler
            throw e; // Re-throw to trigger the error handler
        }
    }
}
