package com.example.clickstream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class ClickCountService {

    private final Map<String, Long> perUserCounts = new ConcurrentHashMap<>();
    private final AtomicLong totalClicks = new AtomicLong();

    @KafkaListener(topics = "${kafka.topics.clickCounts}")
    public void handleClickCount(@Header(KafkaHeaders.RECEIVED_KEY) String userId,
                                 @Payload Long count) {
        long previous = perUserCounts.getOrDefault(userId, 0L);
        perUserCounts.put(userId, count);
        totalClicks.addAndGet(count - previous);
    }

    public long getTotalClicks() {
        return totalClicks.get();
    }

    public Map<String, Long> getPerUserCounts() {
        return Map.copyOf(perUserCounts);
    }
}

