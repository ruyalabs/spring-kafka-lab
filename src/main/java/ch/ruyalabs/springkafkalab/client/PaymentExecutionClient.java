package ch.ruyalabs.springkafkalab.client;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PaymentExecutionClient {

    private static final boolean SIMULATE_PAYMENT_PROCESSING_EXCEPTION = false;
    private static final boolean SIMULATE_INVALID_PAYMENT_METHOD_EXCEPTION = false;
    private static final boolean SIMULATE_GATEWAY_TIMEOUT_EXCEPTION = false;

    /**
     * Simulates a POST request to execute payment
     *
     * @param paymentDto the payment details to process
     * @return payment execution result message
     * @throws PaymentProcessingException    if payment processing fails
     * @throws InvalidPaymentMethodException if payment method is invalid
     * @throws GatewayTimeoutException       if payment gateway times out
     */
    public String executePayment(PaymentDto paymentDto)
            throws PaymentProcessingException, InvalidPaymentMethodException, GatewayTimeoutException {

        log.info("Simulating POST request to execute payment for ID: {} with amount: {} {}",
                paymentDto.getPaymentId(), paymentDto.getAmount(), paymentDto.getCurrency());

        if (SIMULATE_INVALID_PAYMENT_METHOD_EXCEPTION) {
            log.error("Simulating InvalidPaymentMethodException for payment method: {}",
                    paymentDto.getPaymentMethod());
            throw new InvalidPaymentMethodException("Invalid payment method: " + paymentDto.getPaymentMethod());
        }

        if (SIMULATE_GATEWAY_TIMEOUT_EXCEPTION) {
            log.error("Simulating GatewayTimeoutException for payment ID: {}", paymentDto.getPaymentId());
            throw new GatewayTimeoutException("Payment gateway timeout for payment: " + paymentDto.getPaymentId());
        }

        if (SIMULATE_PAYMENT_PROCESSING_EXCEPTION) {
            log.error("Simulating PaymentProcessingException for payment ID: {}", paymentDto.getPaymentId());
            throw new PaymentProcessingException("Payment processing failed for payment: " + paymentDto.getPaymentId() +
                    ". Amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency());
        }

        String result = "Payment executed successfully for ID: " + paymentDto.getPaymentId() +
                " with amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency();
        log.info("Payment execution successful: {}", result);
        return result;
    }

    public static class PaymentProcessingException extends Exception {
        public PaymentProcessingException(String message) {
            super(message);
        }
    }

    public static class InvalidPaymentMethodException extends Exception {
        public InvalidPaymentMethodException(String message) {
            super(message);
        }
    }

    public static class GatewayTimeoutException extends Exception {
        public GatewayTimeoutException(String message) {
            super(message);
        }
    }
}