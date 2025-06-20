package ch.ruyalabs.springkafkalab.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for capturing logs in tests.
 */
public class LogCaptor {
    private final ListAppender<ILoggingEvent> listAppender;
    private final Logger logger;

    /**
     * Creates a new LogCaptor for the specified logger.
     *
     * @param loggerName the name of the logger to capture
     */
    public LogCaptor(String loggerName) {
        logger = (Logger) LoggerFactory.getLogger(loggerName);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
    }

    /**
     * Returns all captured log events.
     *
     * @return the list of captured log events
     */
    public List<ILoggingEvent> getEvents() {
        return listAppender.list;
    }

    /**
     * Returns all captured log messages.
     *
     * @return the list of captured log messages
     */
    public List<String> getMessages() {
        return listAppender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    /**
     * Returns all captured log messages at the specified level.
     *
     * @param level the log level
     * @return the list of captured log messages at the specified level
     */
    public List<String> getMessages(Level level) {
        return listAppender.list.stream()
                .filter(event -> event.getLevel().equals(level))
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
    }

    /**
     * Clears all captured log events.
     */
    public void clear() {
        listAppender.list.clear();
    }

    /**
     * Stops capturing logs and removes the appender.
     */
    public void stop() {
        logger.detachAppender(listAppender);
        listAppender.stop();
    }
}