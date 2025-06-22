package ch.ruyalabs.springkafkalab.exception;

public class GatewayTimeoutException extends Exception {
    public GatewayTimeoutException(String message) {
        super(message);
    }
}