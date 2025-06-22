package ch.ruyalabs.springkafkalab.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BalanceCheckClient {

    private static final boolean SIMULATE_INSUFFICIENT_BALANCE_EXCEPTION = false;
    private static final boolean SIMULATE_ACCOUNT_NOT_FOUND_EXCEPTION = false;
    private static final boolean SIMULATE_SERVICE_UNAVAILABLE_EXCEPTION = false;

    /**
     * Simulates a GET request to check account balance
     *
     * @param customerId     the customer ID to check balance for
     * @param requiredAmount the amount needed for the transaction
     * @return true if balance is sufficient, false otherwise
     * @throws InsufficientBalanceException if balance is insufficient
     * @throws AccountNotFoundException     if account is not found
     * @throws ServiceUnavailableException  if service is unavailable
     */
    public boolean checkBalance(String customerId, Double requiredAmount)
            throws InsufficientBalanceException, AccountNotFoundException, ServiceUnavailableException {

        log.info("Simulating GET request to check balance for customer: {} with required amount: {}",
                customerId, requiredAmount);

        if (SIMULATE_ACCOUNT_NOT_FOUND_EXCEPTION) {
            log.error("Simulating AccountNotFoundException for customer: {}", customerId);
            throw new AccountNotFoundException("Account not found for customer: " + customerId);
        }

        if (SIMULATE_SERVICE_UNAVAILABLE_EXCEPTION) {
            log.error("Simulating ServiceUnavailableException for balance check service");
            throw new ServiceUnavailableException("Balance check service is currently unavailable");
        }

        if (SIMULATE_INSUFFICIENT_BALANCE_EXCEPTION) {
            log.warn("Simulating InsufficientBalanceException for customer: {} with required amount: {}",
                    customerId, requiredAmount);
            throw new InsufficientBalanceException("Insufficient balance for customer: " + customerId +
                    ". Required: " + requiredAmount);
        }

        log.info("Balance check successful for customer: {}. Sufficient balance available.", customerId);
        return true;
    }

    public static class InsufficientBalanceException extends Exception {
        public InsufficientBalanceException(String message) {
            super(message);
        }
    }

    public static class AccountNotFoundException extends Exception {
        public AccountNotFoundException(String message) {
            super(message);
        }
    }

    public static class ServiceUnavailableException extends Exception {
        public ServiceUnavailableException(String message) {
            super(message);
        }
    }
}