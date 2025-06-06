package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.AccountBalance;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.InsufficientFundsException;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.client.BookingClient.BookingDetails;
import ch.ruyalabs.springkafkalab.client.BookingClient.BookingFailedException;
import ch.ruyalabs.springkafkalab.client.BookingClient.BookingResponse;
import ch.ruyalabs.springkafkalab.client.BookingClient.BookingTimeoutException;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

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

    @Value("${payment.booking.confirmation.max-attempts:10}")
    private int maxBookingConfirmationAttempts;

    @Value("${payment.booking.confirmation.delay-ms:500}")
    private long bookingConfirmationDelayMs;

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
        id = "paymentRequestListener",
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

        // Validate payment
        validatePayment(payment);

        // Step 1: Check account balance
        AccountBalance balance = checkAccountBalance(payment);

        // Step 2: Create booking
        BookingResponse bookingResponse = createBooking(payment, balance.accountId());

        // Step 3: Wait for booking confirmation
        BookingDetails confirmedBooking = waitForBookingConfirmation(bookingResponse.bookingId());

        // Step 4: Deduct amount from account
        updateAccountBalance(payment, balance);

        // If we got here, all steps were successful
        log.info("Successfully processed payment: {}", payment.getId());

        // Acknowledge the message
        acknowledgment.acknowledge();
        log.info("Acknowledged payment request: {}", payment.getId());
    }

    /**
     * Validate the payment request
     * 
     * @param payment the payment request
     * @throws IllegalArgumentException if the payment is invalid
     */
    private void validatePayment(PaymentDto payment) {
        if (payment.getId() == null || payment.getId().isEmpty()) {
            throw new IllegalArgumentException("Payment ID is required");
        }

        if (payment.getAmount() == null || payment.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Payment amount must be positive");
        }

        if (payment.getCurrency() == null || payment.getCurrency().isEmpty()) {
            throw new IllegalArgumentException("Payment currency is required");
        }

        log.info("Payment validation successful: {}", payment.getId());
    }

    /**
     * Check account balance for the payment
     * 
     * @param payment the payment request
     * @return the account balance
     * @throws AccountNotFoundException if the account does not exist
     * @throws InsufficientFundsException if there are insufficient funds
     */
    private AccountBalance checkAccountBalance(PaymentDto payment) {
        log.info("Checking account balance for payment: {}", payment.getId());

        // In a real application, we would get the account ID from the payment
        // For demonstration, we'll use a fixed account ID
        String accountId = "account-1"; // Using one of our predefined accounts

        AccountBalance balance = accountBalanceClient.getBalance(accountId);
        log.info("Account balance for payment {}: {} {}", 
                payment.getId(), balance.balance(), balance.currency());

        // Verify sufficient funds
        if (balance.balance().compareTo(payment.getAmount()) < 0) {
            log.error("Insufficient funds for payment {}: {} {} (available: {} {})",
                    payment.getId(), payment.getAmount(), payment.getCurrency(),
                    balance.balance(), balance.currency());
            throw new InsufficientFundsException("Insufficient funds for payment: " + payment.getId());
        }

        // Verify currency match
        if (!balance.currency().equals(payment.getCurrency())) {
            log.error("Currency mismatch for payment {}: {} (account currency: {})",
                    payment.getId(), payment.getCurrency(), balance.currency());
            throw new IllegalArgumentException("Currency mismatch for payment: " + payment.getId());
        }

        return balance;
    }

    /**
     * Create a booking for the payment
     * 
     * @param payment the payment request
     * @param accountId the account ID
     * @return the booking response
     */
    private BookingResponse createBooking(PaymentDto payment, String accountId) {
        log.info("Creating booking for payment: {}", payment.getId());

        BookingClient.BookingRequest bookingRequest = new BookingClient.BookingRequest(
                accountId,
                "Payment: " + payment.getDescription(),
                payment.getAmount().intValue() // Using amount as quantity for demonstration
        );

        BookingResponse bookingResponse = bookingClient.createBooking(bookingRequest);
        log.info("Booking created for payment {}: bookingId={}, status={}",
                payment.getId(), bookingResponse.bookingId(), bookingResponse.status());

        return bookingResponse;
    }

    /**
     * Wait for booking confirmation
     * 
     * @param bookingId the booking ID
     * @return the confirmed booking details
     * @throws BookingFailedException if the booking failed
     * @throws BookingTimeoutException if the booking confirmation timed out
     */
    private BookingDetails waitForBookingConfirmation(String bookingId) {
        log.info("Waiting for booking confirmation: {}", bookingId);

        BookingDetails confirmedBooking = bookingClient.waitForBookingConfirmation(
                bookingId, maxBookingConfirmationAttempts, bookingConfirmationDelayMs);

        log.info("Booking confirmed: {}", bookingId);
        return confirmedBooking;
    }

    /**
     * Update account balance for the payment
     * 
     * @param payment the payment request
     * @param balance the account balance
     * @return the updated account balance
     */
    private AccountBalance updateAccountBalance(PaymentDto payment, AccountBalance balance) {
        log.info("Updating account balance for payment: {}", payment.getId());

        // Deduct the payment amount (negative amount for deduction)
        BigDecimal deductionAmount = payment.getAmount().negate();

        AccountBalance updatedBalance = accountBalanceClient.updateBalance(
                balance.accountId(), deductionAmount, payment.getCurrency());

        log.info("Updated account balance for payment {}: {} {}",
                payment.getId(), updatedBalance.balance(), updatedBalance.currency());

        return updatedBalance;
    }
}
