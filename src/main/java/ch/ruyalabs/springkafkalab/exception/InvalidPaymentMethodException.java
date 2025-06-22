package ch.ruyalabs.springkafkalab.exception;

public class InvalidPaymentMethodException extends Exception {
    public InvalidPaymentMethodException(String message) {
        super(message);
    }
}