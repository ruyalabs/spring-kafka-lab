package ch.ruyalabs.accountbalanceservice.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.math.BigDecimal;

@RestController
@RequestMapping("/api/balance")
public class AccountBalanceController {

    @GetMapping("/{accountId}")
    public AccountBalance getBalance(@PathVariable String accountId) {
        // Dummy implementation
        return new AccountBalance(accountId, new BigDecimal("10000.00"), "USD");
    }

    // Dummy DTO
    record AccountBalance(String accountId, BigDecimal balance, String currency) {}
}