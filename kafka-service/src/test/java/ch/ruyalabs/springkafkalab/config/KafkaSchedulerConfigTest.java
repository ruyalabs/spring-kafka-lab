package ch.ruyalabs.springkafkalab.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaSchedulerConfigTest {

    private static final String PAYMENT_LISTENER_ID = "paymentRequestListener";
    private static final ZoneId ZURICH_ZONE_ID = ZoneId.of("Europe/Zurich");

    @Mock
    private KafkaListenerEndpointRegistry registry;

    @Mock
    private MessageListenerContainer container;

    @InjectMocks
    private KafkaSchedulerConfig kafkaSchedulerConfig;

    // The @BeforeEach setUp method is no longer needed.

    @Test
    void pauseConsumer_shouldPauseTheCorrectContainer() {
        // Arrange
        // Stubbing is now moved inside the test that needs it
        when(registry.getListenerContainer(PAYMENT_LISTENER_ID)).thenReturn(container);
        when(container.isContainerPaused()).thenReturn(false);

        // Act
        kafkaSchedulerConfig.pauseConsumer();

        // Assert
        verify(registry).getListenerContainer(PAYMENT_LISTENER_ID);
        verify(container).pause();
        verify(container, never()).resume();
    }

    @Test
    void resumeConsumer_shouldResumeTheCorrectContainer() {
        // Arrange
        // Stubbing is now moved inside the test that needs it
        when(registry.getListenerContainer(PAYMENT_LISTENER_ID)).thenReturn(container);
        when(container.isContainerPaused()).thenReturn(true);

        // Act
        kafkaSchedulerConfig.resumeConsumer();

        // Assert
        verify(registry).getListenerContainer(PAYMENT_LISTENER_ID);
        verify(container).resume();
        verify(container, never()).pause();
    }

    @Test
    void startupPauser_shouldPauseConsumer_whenStartedDuringMaintenanceWindow() throws Exception {
        // Arrange
        LocalDateTime maintenanceTime = LocalDateTime.of(2024, 7, 1, 3, 30);
        // Stubbing is now moved inside the test that needs it
        when(registry.getListenerContainer(PAYMENT_LISTENER_ID)).thenReturn(container);

        try (MockedStatic<LocalDateTime> mockedStatic = mockStatic(LocalDateTime.class)) {
            mockedStatic.when(() -> LocalDateTime.now(ZURICH_ZONE_ID)).thenReturn(maintenanceTime);

            // Act
            ApplicationRunner runner = kafkaSchedulerConfig.startupPauser();
            runner.run(null);

            // Assert
            verify(container).pause();
        }
    }

    @Test
    void startupPauser_shouldNotPauseConsumer_whenStartedOutsideMaintenanceWindow() throws Exception {
        // Arrange
        LocalDateTime normalTime = LocalDateTime.of(2024, 7, 2, 10, 0);
        // NO stubbing is needed here, as getListenerContainer is never called.

        try (MockedStatic<LocalDateTime> mockedStatic = mockStatic(LocalDateTime.class)) {
            mockedStatic.when(() -> LocalDateTime.now(ZURICH_ZONE_ID)).thenReturn(normalTime);

            // Act
            ApplicationRunner runner = kafkaSchedulerConfig.startupPauser();
            runner.run(null);

            // Assert
            // Verify that the registry was never interacted with.
            verifyNoInteractions(registry);
            verify(container, never()).pause();
        }
    }
}