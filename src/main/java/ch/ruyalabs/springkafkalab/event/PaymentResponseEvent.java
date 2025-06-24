package ch.ruyalabs.springkafkalab.event;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Event published when a payment response needs to be sent after transaction commit.
 * This event carries the payment ID that identifies which pending response should be processed.
 */
@Getter
@RequiredArgsConstructor
public class PaymentResponseEvent {
    private final String paymentId;
}