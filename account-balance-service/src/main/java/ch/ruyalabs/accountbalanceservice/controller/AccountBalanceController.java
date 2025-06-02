package ch.ruyalabs.accountbalanceservice.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.math.BigDecimal;

/**
 * Controller for account balance operations
 */
@Slf4j
@RestController
@RequestMapping("/api/balance")
public class AccountBalanceController {

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
            // Dummy implementation - in a real application, this would query a database
            AccountBalance balance = new AccountBalance(accountId, new BigDecimal("10000.00"), "USD");
            log.info("Retrieved balance for account {}: {}", accountId, balance);
            return new ResponseEntity<>(balance, HttpStatus.OK);
        } catch (Exception e) {
            log.error("Error retrieving balance for account {}: {}", accountId, e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * DTO for account balance information
     */
    public record AccountBalance(String accountId, BigDecimal balance, String currency) {}
}
