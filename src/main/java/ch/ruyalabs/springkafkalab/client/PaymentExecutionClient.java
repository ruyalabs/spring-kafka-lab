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

        log.info("Starting payment execution simulation - Operation: payment_execution, PaymentId: {}, CustomerId: {}, Amount: {} {}, PaymentMethod: {}", 
                paymentDto.getPaymentId(), paymentDto.getCustomerId(), paymentDto.getAmount(), paymentDto.getCurrency(), paymentDto.getPaymentMethod());

        if (simulateInvalidPaymentMethodException) {
            log.error("Payment execution failed due to invalid payment method - ErrorType: invalid_payment_method, PaymentMethod: {}, PaymentId: {}", 
                    paymentDto.getPaymentMethod(), paymentDto.getPaymentId());
            throw new InvalidPaymentMethodException("Invalid payment method: " + paymentDto.getPaymentMethod());
        }

        if (simulateGatewayTimeoutException) {
            log.error("Payment execution failed due to gateway timeout - ErrorType: gateway_timeout, PaymentId: {}", 
                    paymentDto.getPaymentId());
            throw new GatewayTimeoutException("Payment gateway timeout for payment: " + paymentDto.getPaymentId());
        }

        if (simulatePaymentProcessingException) {
            log.error("Payment execution failed due to processing error - ErrorType: processing_error, PaymentId: {}, Amount: {} {}", 
                    paymentDto.getPaymentId(), paymentDto.getAmount(), paymentDto.getCurrency());
            throw new PaymentProcessingException("Payment processing failed for payment: " + paymentDto.getPaymentId() +
                    ". Amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency());
        }

        String result = "Payment executed successfully for ID: " + paymentDto.getPaymentId() +
                " with amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency();

        log.info("Payment execution completed successfully - Result: {}", result);

        return result;
    }

}
