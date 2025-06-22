package ch.ruyalabs.springkafkalab.client;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import ch.ruyalabs.springkafkalab.exception.GatewayTimeoutException;
import ch.ruyalabs.springkafkalab.exception.InvalidPaymentMethodException;
import ch.ruyalabs.springkafkalab.exception.PaymentProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
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

        // Set MDC context for structured logging
        MDC.put("paymentId", paymentDto.getPaymentId());
        MDC.put("customerId", paymentDto.getCustomerId());
        MDC.put("amount", paymentDto.getAmount().toString());
        MDC.put("currency", paymentDto.getCurrency());
        MDC.put("paymentMethod", paymentDto.getPaymentMethod().toString());
        MDC.put("operation", "payment_execution");

        try {
            log.info("Starting payment execution simulation", 
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_execution_start"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentDto.getPaymentId()),
                    net.logstash.logback.argument.StructuredArguments.kv("amount", paymentDto.getAmount()),
                    net.logstash.logback.argument.StructuredArguments.kv("currency", paymentDto.getCurrency()),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentMethod", paymentDto.getPaymentMethod()));

            if (simulateInvalidPaymentMethodException) {
                log.error("Payment execution failed due to invalid payment method",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "payment_execution_failed"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", "invalid_payment_method"),
                        net.logstash.logback.argument.StructuredArguments.kv("paymentMethod", paymentDto.getPaymentMethod()));
                throw new InvalidPaymentMethodException("Invalid payment method: " + paymentDto.getPaymentMethod());
            }

            if (simulateGatewayTimeoutException) {
                log.error("Payment execution failed due to gateway timeout",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "payment_execution_failed"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", "gateway_timeout"));
                throw new GatewayTimeoutException("Payment gateway timeout for payment: " + paymentDto.getPaymentId());
            }

            if (simulatePaymentProcessingException) {
                log.error("Payment execution failed due to processing error",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "payment_execution_failed"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", "processing_error"));
                throw new PaymentProcessingException("Payment processing failed for payment: " + paymentDto.getPaymentId() +
                        ". Amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency());
            }

            String result = "Payment executed successfully for ID: " + paymentDto.getPaymentId() +
                    " with amount: " + paymentDto.getAmount() + " " + paymentDto.getCurrency();

            log.info("Payment execution completed successfully",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_execution_success"),
                    net.logstash.logback.argument.StructuredArguments.kv("result", result));

            return result;
        } finally {
            // Clear MDC context
            MDC.clear();
        }
    }

}
