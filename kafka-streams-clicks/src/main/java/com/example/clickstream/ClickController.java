package com.example.clickstream;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClickController {

    private static final Logger logger = LoggerFactory.getLogger(ClickController.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String clicksTopic;

    public ClickController(KafkaTemplate<String, String> kafkaTemplate,
                           @Value("${kafka.topics.clicks}") String clicksTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.clicksTopic = clicksTopic;
    }

    @GetMapping("/click")
    public String click(@RequestParam(defaultValue = "anonymous") String userId) {
        try {
            logger.info("Attempting to send click for user: {} to topic: {}", userId, clicksTopic);
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(clicksTopic, userId, "click");
            
            // Wait for the result to catch immediate errors (with 5 second timeout)
            try {
                SendResult<String, String> result = future.get(5, TimeUnit.SECONDS);
                logger.info("Click sent successfully for user: {} to topic: {} at offset: {}", 
                    userId, clicksTopic, result.getRecordMetadata().offset());
                return "Click sent for user: " + userId;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                logger.error("Failed to send click for user: {} to topic: {}", userId, clicksTopic, cause);
                return "Error sending click: " + (cause != null ? cause.getMessage() : e.getMessage());
            } catch (TimeoutException e) {
                logger.error("Timeout sending click for user: {} to topic: {}", userId, clicksTopic, e);
                return "Timeout sending click. Message may still be sent.";
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while sending click for user: {} to topic: {}", userId, clicksTopic, e);
                return "Interrupted while sending click.";
            }
        } catch (Exception e) {
            logger.error("Error sending click for user: {}", userId, e);
            return "Error sending click: " + e.getMessage();
        }
    }
}
