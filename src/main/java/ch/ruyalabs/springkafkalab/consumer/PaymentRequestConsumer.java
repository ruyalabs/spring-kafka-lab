package ch.ruyalabs.springkafkalab.consumer;

import ch.ruyalabs.springkafkalab.dto.PaymentDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PaymentRequestConsumer {

    @KafkaListener(topics = "payment-request", groupId = "payment-request-consumer-group")
    public void consume(PaymentDto paymentDto) {
        log.info("Consumed PaymentDto from payment-request topic: {}", paymentDto);
    }
}
