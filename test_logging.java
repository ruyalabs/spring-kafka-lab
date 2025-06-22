import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class test_logging {
    private static final Logger logger = LoggerFactory.getLogger(test_logging.class);
    
    public static void main(String[] args) {
        System.out.println("[DEBUG_LOG] Testing console log format");
        logger.info("This is an INFO log message to test console formatting");
        logger.warn("This is a WARN log message with some data: paymentId=test123, amount=99.99");
        logger.error("This is an ERROR log message to demonstrate multi-line readability");
        System.out.println("[DEBUG_LOG] Console log format test completed");
    }
}