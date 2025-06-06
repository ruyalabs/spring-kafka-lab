package ch.ruyalabs.springkafkalab.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@Slf4j
@Configuration
@EnableScheduling
public class KafkaSchedulerConfig {

    private static final ZoneId ZURICH_ZONE_ID = ZoneId.of("Europe/Zurich");

    private final KafkaListenerEndpointRegistry registry;
    private final List<String> pausableListenerIds;

    public KafkaSchedulerConfig(KafkaListenerEndpointRegistry registry,
                                @Value("${kafka.scheduler.listener-ids}") List<String> pausableListenerIds) {
        this.registry = registry;
        this.pausableListenerIds = pausableListenerIds;
    }

    /**
     * Pauses all registered consumers at midnight on the first day of each month.
     */
    @Scheduled(cron = "0 0 0 1 * ?", zone = "Europe/Zurich")
    public void pauseConsumers() {
        log.info("Scheduled task: Pausing all registered consumers: {}", pausableListenerIds);
        pausableListenerIds.forEach(this::pauseListener);
    }

    /**
     * Resumes all registered consumers at 6 AM on the first day of each month.
     */
    @Scheduled(cron = "0 0 6 1 * ?", zone = "Europe/Zurich")
    public void resumeConsumers() {
        log.info("Scheduled task: Resuming all registered consumers: {}", pausableListenerIds);
        pausableListenerIds.forEach(this::resumeListener);
    }

    /**
     * On application startup, checks if consumers should be paused.
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
                log.info("Application started within the maintenance window. Pausing consumers.");
                pausableListenerIds.forEach(this::pauseListener);
            } else {
                log.info("Application started outside the maintenance window. Consumers will start normally.");
            }
        };
    }

    private void pauseListener(String listenerId) {
        log.info("Attempting to pause consumer with ID '{}'", listenerId);
        try {
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            if (container != null) {
                if (container.isPauseRequested() || container.isContainerPaused()) {
                    log.warn("Container '{}' is already paused or has a pause requested.", listenerId);
                } else {
                    container.pause();
                    log.info("Container '{}' paused successfully.", listenerId);
                }
            } else {
                log.warn("Could not find listener container with ID '{}' to pause.", listenerId);
            }
        } catch (Exception e) {
            log.error("Error pausing Kafka listener container '{}': {}", listenerId, e.getMessage(), e);
        }
    }

    private void resumeListener(String listenerId) {
        log.info("Attempting to resume consumer with ID '{}'", listenerId);
        try {
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            if (container != null) {
                if (container.isContainerPaused()) {
                    container.resume();
                    log.info("Container '{}' resumed successfully.", listenerId);
                } else {
                    log.warn("Container '{}' is not paused, no action taken.", listenerId);
                }
            } else {
                log.warn("Could not find listener container with ID '{}' to resume.", listenerId);
            }
        } catch (Exception e) {
            log.error("Error resuming Kafka listener container '{}': {}", listenerId, e.getMessage(), e);
        }
    }
}