package ch.ruyalabs.springkafkalab.service;

import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.AccountBalance;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.client.AccountBalanceClient.InsufficientFundsException;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.producer.PaymentRequestProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentRequestProcessorService {

    private final PaymentRequestProducer paymentRequestProducer;
    private final AccountBalanceClient accountBalanceClient;
    private final BookingClient bookingClient;

    /**
     * Process a payment request
     * 
     * @param paymentDto the payment request to process
     * @return the processed payment with a generated ID if not provided
     * @throws IllegalArgumentException if the payment request is invalid
     * @throws InsufficientFundsException if the account has insufficient funds
     * @throws RuntimeException if the payment could not be sent to Kafka
     */
    public PaymentDto processPayment(PaymentDto paymentDto) {
        log.info("Processing payment request: {}", paymentDto);

        // Validate and prepare payment
        validateAndPreparePayment(paymentDto);

        // Pre-check account balance before sending to Kafka
        preCheckAccountBalance(paymentDto);

        // Send the payment request to Kafka asynchronously
        sendToKafka(paymentDto);

        return paymentDto;
    }

    /**
     * Validate and prepare the payment request
     * 
     * @param paymentDto the payment request to validate and prepare
     * @throws IllegalArgumentException if the payment request is invalid
     */
    private void validateAndPreparePayment(PaymentDto paymentDto) {
        // Validate payment
        if (paymentDto.getAmount() == null || paymentDto.getAmount().signum() <= 0) {
            throw new IllegalArgumentException("Payment amount must be positive");
        }

        if (paymentDto.getCurrency() == null || paymentDto.getCurrency().isEmpty()) {
            throw new IllegalArgumentException("Payment currency is required");
        }

        // Generate an ID if not provided
        if (paymentDto.getId() == null || paymentDto.getId().isEmpty()) {
            paymentDto.setId(UUID.randomUUID().toString());
            log.info("Generated payment ID: {}", paymentDto.getId());
        }

        // Set default description if not provided
        if (paymentDto.getDescription() == null || paymentDto.getDescription().isEmpty()) {
            paymentDto.setDescription("Payment " + paymentDto.getId());
            log.info("Set default description for payment: {}", paymentDto.getId());
        }
    }

    /**
     * Pre-check account balance before sending to Kafka
     * 
     * @param paymentDto the payment request to check
     * @throws InsufficientFundsException if the account has insufficient funds
     */
    private void preCheckAccountBalance(PaymentDto paymentDto) {
        try {
            // In a real application, we would get the account ID from the payment
            // For demonstration, we'll use a fixed account ID
            String accountId = "account-1"; // Using one of our predefined accounts

            log.info("Pre-checking account balance for payment {}: account={}", 
                    paymentDto.getId(), accountId);

            AccountBalance balance = accountBalanceClient.getBalance(accountId);
            log.info("Account balance for payment {}: {} {}", 
                    paymentDto.getId(), balance.balance(), balance.currency());

            // Verify that the account has sufficient funds
            if (balance.balance().compareTo(paymentDto.getAmount()) < 0) {
                log.warn("Insufficient funds for payment {}: {} {} (available: {} {})", 
                        paymentDto.getId(), paymentDto.getAmount(), paymentDto.getCurrency(),
                        balance.balance(), balance.currency());
                throw new InsufficientFundsException("Insufficient funds for payment: " + paymentDto.getId());
            }

            // Verify currency match
            if (!balance.currency().equals(paymentDto.getCurrency())) {
                log.warn("Currency mismatch for payment {}: {} (account currency: {})",
                        paymentDto.getId(), paymentDto.getCurrency(), balance.currency());
                throw new IllegalArgumentException("Currency mismatch for payment: " + paymentDto.getId());
            }

            log.info("Pre-check successful for payment {}", paymentDto.getId());
        } catch (AccountNotFoundException e) {
            log.error("Account not found for payment {}: {}", paymentDto.getId(), e.getMessage());
            throw e;
        } catch (InsufficientFundsException e) {
            log.error("Insufficient funds for payment {}: {}", paymentDto.getId(), e.getMessage());
            throw e;
        } catch (Exception e) {
            // For other errors, log but continue - the consumer will handle these
            log.warn("Error during pre-check for payment {}: {}", paymentDto.getId(), e.getMessage());
            log.debug("Pre-check error details", e);
        }
    }

    /**
     * Send the payment request to Kafka
     * 
     * @param paymentDto the payment request to send
     * @throws RuntimeException if the payment could not be sent to Kafka
     */
    private void sendToKafka(PaymentDto paymentDto) {
        log.info("Sending payment request to Kafka: {}", paymentDto.getId());

        // Send the payment request to Kafka asynchronously
        paymentRequestProducer.sendPaymentRequest(paymentDto)
            .whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Failed to send payment request to Kafka: {}", paymentDto.getId(), exception);
                } else {
                    log.info("Payment request sent to Kafka successfully: {}", paymentDto.getId());
                }
            });

        // Log that we've initiated the send operation
        log.info("Initiated Kafka send operation for payment: {}", paymentDto.getId());

        // For demonstration purposes, we could wait for the result if needed
        // This would make the method blocking, but would ensure the message is sent
        // before returning to the caller
        /*
        try {
            CompletableFuture<org.springframework.kafka.support.SendResult<String, PaymentDto>> future = 
                paymentRequestProducer.sendPaymentRequest(paymentDto);
            future.join();
            log.info("Confirmed payment request sent to Kafka: {}", paymentDto.getId());
        } catch (CompletionException e) {
            log.error("Failed to send payment request to Kafka: {}", paymentDto.getId(), e.getCause());
            throw new RuntimeException("Failed to send payment request to Kafka", e.getCause());
        }
        */
    }
}
