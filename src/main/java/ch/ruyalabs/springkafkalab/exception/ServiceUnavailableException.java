package ch.ruyalabs.springkafkalab.exception;

public class ServiceUnavailableException extends Exception {
    public ServiceUnavailableException(String message) {
        super(message);
    }
}