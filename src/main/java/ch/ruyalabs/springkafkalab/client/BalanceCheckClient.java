package ch.ruyalabs.springkafkalab.client;

import ch.ruyalabs.springkafkalab.exception.AccountNotFoundException;
import ch.ruyalabs.springkafkalab.exception.InsufficientBalanceException;
import ch.ruyalabs.springkafkalab.exception.ServiceUnavailableException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
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

        // Set MDC context for structured logging
        MDC.put("customerId", customerId);
        MDC.put("requiredAmount", requiredAmount.toString());
        MDC.put("operation", "balance_check");

        try {
            log.info("Starting balance check simulation",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "balance_check_start"),
                    net.logstash.logback.argument.StructuredArguments.kv("customerId", customerId),
                    net.logstash.logback.argument.StructuredArguments.kv("requiredAmount", requiredAmount));

            if (simulateAccountNotFoundException) {
                log.error("Balance check failed due to account not found",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "balance_check_failed"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", "account_not_found"),
                        net.logstash.logback.argument.StructuredArguments.kv("customerId", customerId));
                throw new AccountNotFoundException("Account not found for customer: " + customerId);
            }

            if (simulateServiceUnavailableException) {
                log.error("Balance check failed due to service unavailable",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "balance_check_failed"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", "service_unavailable"));
                throw new ServiceUnavailableException("Balance check service is currently unavailable");
            }

            if (simulateInsufficientBalanceException) {
                log.warn("Balance check failed due to insufficient balance",
                        net.logstash.logback.argument.StructuredArguments.kv("event", "balance_check_failed"),
                        net.logstash.logback.argument.StructuredArguments.kv("errorType", "insufficient_balance"),
                        net.logstash.logback.argument.StructuredArguments.kv("customerId", customerId),
                        net.logstash.logback.argument.StructuredArguments.kv("requiredAmount", requiredAmount));
                throw new InsufficientBalanceException("Insufficient balance for customer: " + customerId +
                        ". Required: " + requiredAmount);
            }

            log.info("Balance check completed successfully",
                    net.logstash.logback.argument.StructuredArguments.kv("event", "balance_check_success"),
                    net.logstash.logback.argument.StructuredArguments.kv("customerId", customerId),
                    net.logstash.logback.argument.StructuredArguments.kv("balanceStatus", "sufficient"));

            return true;
        } finally {
            // Clear MDC context
            MDC.clear();
        }
    }

}
