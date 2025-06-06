package ch.ruyalabs.springkafkalab.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Slf4j
@Configuration
@EnableScheduling
public class KafkaSchedulerConfig {

    private static final String PAYMENT_LISTENER_ID = "paymentRequestListener";

    // Direct dependency injection is preferred
    private final KafkaListenerEndpointRegistry registry;

    // The message from IntelliJ is a "false negative" warning
    public KafkaSchedulerConfig(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    /**
     * Pauses the PaymentRequestConsumer at midnight on the first day of each month.
     */
    @Scheduled(cron = "0 0 0 1 * ?")
    public void pauseConsumer() {
        log.info("Scheduled task: Pausing consumer with ID '{}'", PAYMENT_LISTENER_ID);
        try {
            MessageListenerContainer container = registry.getListenerContainer(PAYMENT_LISTENER_ID);
            if (container != null) {
                if (container.isPauseRequested()) {
                    log.warn("Container '{}' is already paused.", PAYMENT_LISTENER_ID);
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
    @Scheduled(cron = "0 0 6 1 * ?")
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
}