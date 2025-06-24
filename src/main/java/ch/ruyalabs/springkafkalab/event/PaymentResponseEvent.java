package ch.ruyalabs.springkafkalab.event;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


@Getter
@RequiredArgsConstructor
public class PaymentResponseEvent {
    private final String paymentId;
}