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

    public void requestPaymentExecution(PaymentDto paymentDto) {

        log.info("Requesting payment execution - Operation: payment_execution_request, PaymentId: {}, CustomerId: {}, Amount: {} {}, PaymentMethod: {}",
                paymentDto.getPaymentId(), paymentDto.getCustomerId(), paymentDto.getAmount(), paymentDto.getCurrency(), paymentDto.getPaymentMethod());

        // This is now just a request - the actual execution will be handled by an external system
        // The external system will write the result to the payment-execution-status topic

        log.info("Payment execution request submitted successfully - PaymentId: {}, CustomerId: {}, Amount: {} {}",
                paymentDto.getPaymentId(), paymentDto.getCustomerId(), paymentDto.getAmount(), paymentDto.getCurrency());
    }

}
