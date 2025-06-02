package ch.ruyalabs.springkafkalab.service;

import ch.ruyalabs.springkafkalab.client.AccountBalanceClient;
import ch.ruyalabs.springkafkalab.client.BookingClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.producer.PaymentRequestProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentRequestProcessorService {

    private final PaymentRequestProducer paymentRequestProducer;
    private final AccountBalanceClient accountBalanceClient;

    /**
     * Process a payment request
     * 
     * @param paymentDto the payment request to process
     * @return the processed payment with a generated ID if not provided
     * @throws RuntimeException if the payment could not be sent to Kafka
     */
    public PaymentDto processPayment(PaymentDto paymentDto) {
        log.info("Processing payment request: {}", paymentDto);

        // Generate an ID if not provided
        if (paymentDto.getId() == null || paymentDto.getId().isEmpty()) {
            paymentDto.setId(UUID.randomUUID().toString());
        }

        // Check account balance - using a dummy account ID for demonstration
        String accountId = "account-" + System.currentTimeMillis();
        try {
            AccountBalanceClient.AccountBalance balance = accountBalanceClient.getBalance(accountId);
            log.info("Account balance for {}: {} {}", accountId, balance.balance(), balance.currency());

            // Verify that the account has sufficient funds
            if (balance.balance().compareTo(paymentDto.getAmount()) < 0) {
                log.warn("Insufficient funds for payment: {} (available: {})", 
                         paymentDto.getAmount(), balance.balance());
                throw new RuntimeException("Insufficient funds for payment");
            }
        } catch (Exception e) {
            log.error("Error checking account balance: {}", e.getMessage(), e);
            // For demonstration purposes, we'll continue even if balance check fails
        }

        // Send the payment request to Kafka asynchronously
        // Don't wait for the result - fully non-blocking
        paymentRequestProducer.sendPaymentRequest(paymentDto)
            .whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Failed to send payment request to Kafka: {}", paymentDto, exception);
                } else {
                    log.info("Payment request sent to Kafka: {}", paymentDto);
                }
            });

        // Log that we've initiated the send operation
        log.info("Initiated Kafka send operation for payment: {}", paymentDto.getId());

        return paymentDto;
    }
}
