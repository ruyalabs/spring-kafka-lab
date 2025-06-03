package ch.ruyalabs.accountbalanceservice.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Controller for account balance operations
 */
@Slf4j
@RestController
@RequestMapping("/api/balance")
public class AccountBalanceController {

    // In-memory storage for account balances
    private final Map<String, AccountBalance> accountBalances = new ConcurrentHashMap<>();

    // Initialize with some sample accounts
    {
        accountBalances.put("account-1", new AccountBalance("account-1", new BigDecimal("10000.00"), "USD"));
        accountBalances.put("account-2", new AccountBalance("account-2", new BigDecimal("5000.00"), "EUR"));
        accountBalances.put("account-3", new AccountBalance("account-3", new BigDecimal("7500.00"), "GBP"));
    }

    /**
     * Get the balance for an account
     *
     * @param accountId the account ID
     * @return ResponseEntity with the account balance
     */
    @GetMapping("/{accountId}")
    public ResponseEntity<AccountBalance> getBalance(@PathVariable String accountId) {
        log.info("Getting balance for account: {}", accountId);

        try {
            AccountBalance balance = accountBalances.get(accountId);

            if (balance == null) {
                log.warn("Account not found: {}", accountId);
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }

            log.info("Retrieved balance for account {}: {}", accountId, balance);
            return new ResponseEntity<>(balance, HttpStatus.OK);
        } catch (Exception e) {
            log.error("Error retrieving balance for account {}: {}", accountId, e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Create a new account with initial balance
     *
     * @param request the account creation request
     * @return ResponseEntity with the created account balance
     */
    @PostMapping
    public ResponseEntity<AccountBalance> createAccount(@RequestBody AccountCreationRequest request) {
        log.info("Creating account with request: {}", request);

        try {
            String accountId = "account-" + System.currentTimeMillis();
            AccountBalance newBalance = new AccountBalance(
                accountId, 
                request.initialBalance(), 
                request.currency()
            );

            accountBalances.put(accountId, newBalance);
            log.info("Created account: {}", newBalance);
            return new ResponseEntity<>(newBalance, HttpStatus.CREATED);
        } catch (Exception e) {
            log.error("Error creating account: {}", e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Update an account balance
     *
     * @param accountId the account ID
     * @param request the balance update request
     * @return ResponseEntity with the updated account balance
     */
    @PutMapping("/{accountId}")
    public ResponseEntity<AccountBalance> updateBalance(
            @PathVariable String accountId,
            @RequestBody BalanceUpdateRequest request) {
        log.info("Updating balance for account {}: {}", accountId, request);

        try {
            AccountBalance currentBalance = accountBalances.get(accountId);

            if (currentBalance == null) {
                log.warn("Account not found for update: {}", accountId);
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }

            // Validate currency match
            if (!currentBalance.currency().equals(request.currency())) {
                log.warn("Currency mismatch for account {}: {} vs {}", 
                    accountId, currentBalance.currency(), request.currency());
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            // Calculate new balance
            BigDecimal newAmount = currentBalance.balance().add(request.amount());

            // Check for negative balance if it's a deduction
            if (request.amount().compareTo(BigDecimal.ZERO) < 0 && 
                newAmount.compareTo(BigDecimal.ZERO) < 0) {
                log.warn("Insufficient funds for account {}: {} (available: {})", 
                    accountId, request.amount().abs(), currentBalance.balance());
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            // Update the balance
            AccountBalance updatedBalance = new AccountBalance(
                accountId, 
                newAmount, 
                currentBalance.currency()
            );

            accountBalances.put(accountId, updatedBalance);
            log.info("Updated balance for account {}: {}", accountId, updatedBalance);
            return new ResponseEntity<>(updatedBalance, HttpStatus.OK);
        } catch (Exception e) {
            log.error("Error updating balance for account {}: {}", accountId, e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
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
}
