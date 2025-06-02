package ch.ruyalabs.springkafkalab.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
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
     */
    public AccountBalance getBalance(String accountId) {
        log.info("Getting balance for account: {}", accountId);
        String url = accountBalanceServiceUrl + "/api/balance/" + accountId;
        
        try {
            AccountBalance balance = restTemplate.getForObject(url, AccountBalance.class);
            log.info("Retrieved balance for account {}: {}", accountId, balance);
            return balance;
        } catch (Exception e) {
            log.error("Error retrieving balance for account {}: {}", accountId, e.getMessage(), e);
            throw new RuntimeException("Failed to retrieve account balance", e);
        }
    }

    /**
     * DTO for account balance information
     */
    public record AccountBalance(String accountId, BigDecimal balance, String currency) {}
}