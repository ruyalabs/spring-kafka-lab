package ch.ruyalabs.springkafkalab.client;

import ch.ruyalabs.springkafkalab.exception.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.exception.InsufficientBalanceException;
import ch.ruyalabs.springkafkalab.exception.ServiceUnavailableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BalanceCheckClient {

    @Value("${app.simulation.balance-check.simulate-insufficient-balance}")
    private boolean simulateInsufficientBalanceException;

    @Value("${app.simulation.balance-check.simulate-account-not-found}")
    private boolean simulateAccountNotFoundException;

    @Value("${app.simulation.balance-check.simulate-service-unavailable}")
    private boolean simulateServiceUnavailableException;

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

        if (simulateAccountNotFoundException) {
            log.error("Simulating AccountNotFoundException for customer: {}", customerId);
            throw new AccountNotFoundException("Account not found for customer: " + customerId);
        }

        if (simulateServiceUnavailableException) {
            log.error("Simulating ServiceUnavailableException for balance check service");
            throw new ServiceUnavailableException("Balance check service is currently unavailable");
        }

        if (simulateInsufficientBalanceException) {
            log.warn("Simulating InsufficientBalanceException for customer: {} with required amount: {}",
                    customerId, requiredAmount);
            throw new InsufficientBalanceException("Insufficient balance for customer: " + customerId +
                    ". Required: " + requiredAmount);
        }

        log.info("Balance check successful for customer: {}. Sufficient balance available.", customerId);
        return true;
    }

}
