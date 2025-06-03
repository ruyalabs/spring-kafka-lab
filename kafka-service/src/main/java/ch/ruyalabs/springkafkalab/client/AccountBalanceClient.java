package ch.ruyalabs.springkafkalab.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

/**
 * Client for the Account Balance Service
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountBalanceClient {

    private final RestTemplate restTemplate;

    @Value("${services.account-balance.url:http://localhost:8081}")
    private String accountBalanceServiceUrl;

    /**
     * Get the balance for an account
     *
     * @param accountId the account ID
     * @return the account balance information
     * @throws AccountNotFoundException if the account does not exist
     * @throws ServiceException if there is an error communicating with the service
     */
    public AccountBalance getBalance(String accountId) {
        log.info("Getting balance for account: {}", accountId);
        String url = accountBalanceServiceUrl + "/api/balance/" + accountId;

        try {
            ResponseEntity<AccountBalance> response = restTemplate.getForEntity(url, AccountBalance.class);
            AccountBalance balance = response.getBody();
            log.info("Retrieved balance for account {}: {}", accountId, balance);
            return balance;
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Account not found: {}", accountId);
                throw new AccountNotFoundException("Account not found: " + accountId);
            }
            log.error("Error retrieving balance for account {}: {} - {}", 
                    accountId, e.getStatusCode(), e.getMessage());
            throw new ServiceException("Error retrieving account balance: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error retrieving balance for account {}: {}", accountId, e.getMessage(), e);
            throw new ServiceException("Failed to retrieve account balance", e);
        }
    }

    /**
     * Create a new account
     *
     * @param initialBalance the initial balance
     * @param currency the currency
     * @return the created account balance information
     * @throws ServiceException if there is an error communicating with the service
     */
    public AccountBalance createAccount(BigDecimal initialBalance, String currency) {
        log.info("Creating account with initial balance {} {}", initialBalance, currency);
        String url = accountBalanceServiceUrl + "/api/balance";

        try {
            AccountCreationRequest request = new AccountCreationRequest(initialBalance, currency);
            ResponseEntity<AccountBalance> response = restTemplate.postForEntity(url, request, AccountBalance.class);
            AccountBalance newAccount = response.getBody();
            log.info("Created account: {}", newAccount);
            return newAccount;
        } catch (Exception e) {
            log.error("Error creating account: {}", e.getMessage(), e);
            throw new ServiceException("Failed to create account", e);
        }
    }

    /**
     * Update an account balance
     *
     * @param accountId the account ID
     * @param amount the amount to add (positive) or subtract (negative)
     * @param currency the currency
     * @return the updated account balance information
     * @throws AccountNotFoundException if the account does not exist
     * @throws InsufficientFundsException if there are insufficient funds for a deduction
     * @throws CurrencyMismatchException if the currency does not match the account currency
     * @throws ServiceException if there is an error communicating with the service
     */
    public AccountBalance updateBalance(String accountId, BigDecimal amount, String currency) {
        log.info("Updating balance for account {}: {} {}", accountId, amount, currency);
        String url = accountBalanceServiceUrl + "/api/balance/" + accountId;

        try {
            BalanceUpdateRequest request = new BalanceUpdateRequest(amount, currency);
            ResponseEntity<AccountBalance> response = restTemplate.exchange(
                url, 
                org.springframework.http.HttpMethod.PUT, 
                new org.springframework.http.HttpEntity<>(request), 
                AccountBalance.class
            );
            AccountBalance updatedBalance = response.getBody();
            log.info("Updated balance for account {}: {}", accountId, updatedBalance);
            return updatedBalance;
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
                log.warn("Account not found for update: {}", accountId);
                throw new AccountNotFoundException("Account not found: " + accountId);
            } else if (e.getStatusCode() == HttpStatus.BAD_REQUEST) {
                // Try to determine the specific error
                if (e.getResponseBodyAsString().contains("Currency mismatch")) {
                    throw new CurrencyMismatchException("Currency mismatch for account " + accountId);
                } else if (e.getResponseBodyAsString().contains("Insufficient funds")) {
                    throw new InsufficientFundsException("Insufficient funds for account " + accountId);
                }
                throw new ServiceException("Bad request: " + e.getMessage(), e);
            }
            log.error("Error updating balance for account {}: {} - {}", 
                    accountId, e.getStatusCode(), e.getMessage());
            throw new ServiceException("Error updating account balance: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error updating balance for account {}: {}", accountId, e.getMessage(), e);
            throw new ServiceException("Failed to update account balance", e);
        }
    }

    /**
     * DTO for account balance information
     */
    public record AccountBalance(String accountId, BigDecimal balance, String currency) {}

    /**
     * DTO for account creation request
     */
    public record AccountCreationRequest(BigDecimal initialBalance, String currency) {}

    /**
     * DTO for balance update request
     */
    public record BalanceUpdateRequest(BigDecimal amount, String currency) {}

    /**
     * Exception thrown when an account is not found
     */
    public static class AccountNotFoundException extends RuntimeException {
        public AccountNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when there are insufficient funds for a deduction
     */
    public static class InsufficientFundsException extends RuntimeException {
        public InsufficientFundsException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when the currency does not match the account currency
     */
    public static class CurrencyMismatchException extends RuntimeException {
        public CurrencyMismatchException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when there is an error communicating with the service
     */
    public static class ServiceException extends RuntimeException {
        public ServiceException(String message) {
            super(message);
        }

        public ServiceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
