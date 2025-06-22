package ch.ruyalabs.springkafkalab.client;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.GatewayTimeoutException;
import ch.ruyalabs.springkafkalab.exception.InvalidPaymentMethodException;
import ch.ruyalabs.springkafkalab.exception.PaymentProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PaymentExecutionClient {

    @Value("${app.simulation.payment-execution.simulate-payment-processing-exception}")
    private boolean simulatePaymentProcessingException;

    @Value("${app.simulation.payment-execution.simulate-invalid-payment-method-exception}")
    private boolean simulateInvalidPaymentMethodException;

    @Value("${app.simulation.payment-execution.simulate-gateway-timeout-exception}")
    private boolean simulateGatewayTimeoutException;

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

        if (simulateInvalidPaymentMethodException) {
            log.error("Simulating InvalidPaymentMethodException for payment method: {}",
                    paymentDto.getPaymentMethod());
            throw new InvalidPaymentMethodException("Invalid payment method: " + paymentDto.getPaymentMethod());
        }

        if (simulateGatewayTimeoutException) {
            log.error("Simulating GatewayTimeoutException for payment ID: {}", paymentDto.getPaymentId());
            throw new GatewayTimeoutException("Payment gateway timeout for payment: " + paymentDto.getPaymentId());
        }

        if (simulatePaymentProcessingException) {
            log.error("Simulating PaymentProcessingException for payment ID: {}", paymentDto.getPaymentId());
            throw new PaymentProcessingException("Payment processing failed for payment: " + paymentDto.getPaymentId() +
                    ". Amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency());
        }

        String result = "Payment executed successfully for ID: " + paymentDto.getPaymentId() +
                " with amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency();
        log.info("Payment execution successful: {}", result);
        return result;
    }

}
