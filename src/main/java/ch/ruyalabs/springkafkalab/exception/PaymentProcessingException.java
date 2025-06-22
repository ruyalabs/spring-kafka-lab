package ch.ruyalabs.springkafkalab.exception;

public class PaymentProcessingException extends Exception {
    public PaymentProcessingException(String message) {
        super(message);
    }
}