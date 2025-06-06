package ch.ruyalabs.springkafkalab.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@Configuration
@EnableScheduling
public class KafkaSchedulerConfig {

    private static final String PAYMENT_LISTENER_ID = "paymentRequestListener";
    private static final ZoneId ZURICH_ZONE_ID = ZoneId.of("Europe/Zurich");

    private final KafkaListenerEndpointRegistry registry;

    public KafkaSchedulerConfig(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    /**
     * Pauses the PaymentRequestConsumer at midnight on the first day of each month.
     */
    @Scheduled(cron = "0 0 0 1 * ?", zone = "Europe/Zurich")
    public void pauseConsumer() {
        log.info("Scheduled task: Pausing consumer with ID '{}'", PAYMENT_LISTENER_ID);
        try {
            MessageListenerContainer container = registry.getListenerContainer(PAYMENT_LISTENER_ID);
            if (container != null) {
                if (container.isPauseRequested() || container.isContainerPaused()) {
                    log.warn("Container '{}' is already paused or has a pause requested.", PAYMENT_LISTENER_ID);
                } else {
                    container.pause();
                    log.info("Container '{}' paused successfully.", PAYMENT_LISTENER_ID);
                }
            } else {
                log.warn("Could not find listener container with ID '{}' to pause.", PAYMENT_LISTENER_ID);
            }
        } catch (Exception e) {
            log.error("Error pausing Kafka listener container '{}': {}", PAYMENT_LISTENER_ID, e.getMessage(), e);
        }
    }

    /**
     * Resumes the PaymentRequestConsumer at 6 AM on the first day of each month.
     */
    @Scheduled(cron = "0 0 6 1 * ?", zone = "Europe/Zurich")
    public void resumeConsumer() {
        log.info("Scheduled task: Resuming consumer with ID '{}'", PAYMENT_LISTENER_ID);
        try {
            MessageListenerContainer container = registry.getListenerContainer(PAYMENT_LISTENER_ID);
            if (container != null) {
                if (container.isContainerPaused()) {
                    container.resume();
                    log.info("Container '{}' resumed successfully.", PAYMENT_LISTENER_ID);
                } else {
                    log.warn("Container '{}' is not paused, no action taken.", PAYMENT_LISTENER_ID);
                }
            } else {
                log.warn("Could not find listener container with ID '{}' to resume.", PAYMENT_LISTENER_ID);
            }
        } catch (Exception e) {
            log.error("Error resuming Kafka listener container '{}': {}", PAYMENT_LISTENER_ID, e.getMessage(), e);
        }
    }

    /**
     * On application startup, checks if the consumer should be paused.
     * This makes the maintenance window resilient to application restarts.
     *
     * @return ApplicationRunner bean
     */
    @Bean
    public ApplicationRunner startupPauser() {
        return args -> {
            LocalDateTime now = LocalDateTime.now(ZURICH_ZONE_ID);

            // Check if we are on the first day of the month, between midnight and 6 AM
            boolean inMaintenanceWindow = now.getDayOfMonth() == 1 && now.getHour() < 6;

            if (inMaintenanceWindow) {
                log.info("Application started within the maintenance window. Pausing consumer '{}'.", PAYMENT_LISTENER_ID);
                MessageListenerContainer container = registry.getListenerContainer(PAYMENT_LISTENER_ID);
                if (container != null) {
                    container.pause();
                    log.info("Container '{}' paused on startup.", PAYMENT_LISTENER_ID);
                } else {
                    log.warn("Could not find listener container with ID '{}' to pause on startup.", PAYMENT_LISTENER_ID);
                }
            } else {
                log.info("Application started outside the maintenance window. Consumer will start normally.");
            }
        };
    }
}