package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.client.BalanceCheckClient;
import ch.ruyalabs.springkafkalab.client.PaymentExecutionClient;
import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRequestConsumer {

    private final BalanceCheckClient balanceCheckClient;
    private final PaymentExecutionClient paymentExecutionClient;
    private final PaymentResponseProducer paymentResponseProducer;

    @KafkaListener(
            topics = "${app.kafka.topics.payment-request}", 
            containerFactory = "paymentRequestKafkaListenerContainerFactory"
    )
    @Transactional(transactionManager = "kafkaTransactionManager")
    public void consume(@Payload @Valid PaymentDto paymentDto, Acknowledgment acknowledgment) 
            throws Exception {

        // Set MDC context for structured logging
        MDC.put("paymentId", paymentDto.getPaymentId());
        MDC.put("customerId", paymentDto.getCustomerId());
        MDC.put("amount", paymentDto.getAmount().toString());
        MDC.put("currency", paymentDto.getCurrency());
        MDC.put("operation", "payment_request_processing");

        try {
            log.info("Payment request consumed from Kafka topic",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "payment_request_consumed"),
                    net.logstash.logback.argument.StructuredArguments.kv("topic", "payment-request"),
                    net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentDto.getPaymentId()),
                    net.logstash.logback.argument.StructuredArguments.kv("customerId", paymentDto.getCustomerId()),
                    net.logstash.logback.argument.StructuredArguments.kv("amount", paymentDto.getAmount()),
                    net.logstash.logback.argument.StructuredArguments.kv("currency", paymentDto.getCurrency()));

            boolean balanceCheckResult = balanceCheckClient.checkBalance(
                    paymentDto.getCustomerId(),
                    paymentDto.getAmount()
            );

            if (balanceCheckResult) {
                paymentExecutionClient.executePayment(paymentDto);
                log.info("Payment request processed successfully",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "payment_request_processed"),
                        net.logstash.logback.argument.StructuredArguments.kv("status", "success"),
                        net.logstash.logback.argument.StructuredArguments.kv("customerId", paymentDto.getCustomerId()));
                // Send success response
                paymentResponseProducer.sendSuccessResponse(paymentDto);
                // Acknowledge the message after successful response is sent
                acknowledgment.acknowledge();
                log.info("Payment request message acknowledged after successful processing",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "message_acknowledged"),
                        net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentDto.getPaymentId()));
            } else {
                log.info("Payment request skipped due to insufficient balance",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "payment_request_skipped"),
                        net.logstash.logback.argument.StructuredArguments.kv("reason", "insufficient_balance"),
                        net.logstash.logback.argument.StructuredArguments.kv("customerId", paymentDto.getCustomerId()));
                // Send error response for insufficient balance
                paymentResponseProducer.sendErrorResponse(paymentDto, "Insufficient balance for payment");
                // Acknowledge the message after error response is sent
                acknowledgment.acknowledge();
                log.info("Payment request message acknowledged after error response sent",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "message_acknowledged"),
                        net.logstash.logback.argument.StructuredArguments.kv("paymentId", paymentDto.getPaymentId()));
            }
        } finally {
            // Clear MDC context
            MDC.clear();
        }
    }
}
