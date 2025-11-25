package com.example.clickstream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;

@Service
public class ClickCountService {

    private final Map<String, Long> perUserCounts = new ConcurrentHashMap<>();
    private final Map<String, Gauge> userGauges = new ConcurrentHashMap<>();
    private final AtomicLong totalClicks = new AtomicLong();
    private final MeterRegistry meterRegistry;
    private Counter clickCounter;

    public ClickCountService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void initMetrics() {
        // Counter for total clicks
        clickCounter = Counter.builder("clicks.total")
            .description("Total number of clicks")
            .register(meterRegistry);

        // Gauge for current total clicks
        Gauge.builder("clicks.current_total", totalClicks, AtomicLong::get)
            .description("Current total number of clicks")
            .register(meterRegistry);

        // Gauge for number of unique users
        Gauge.builder("clicks.unique_users", this, service -> service.perUserCounts.size())
            .description("Number of unique users who clicked")
            .register(meterRegistry);
    }

    @KafkaListener(topics = "${kafka.topics.clickCounts}")
    public void handleClickCount(@Header(KafkaHeaders.RECEIVED_KEY) String userId,
                                 @Payload Long count) {
        long previous = perUserCounts.getOrDefault(userId, 0L);
        perUserCounts.put(userId, count);
        long increment = count - previous;
        totalClicks.addAndGet(increment);

        // Update Prometheus metrics
        if (increment > 0) {
            clickCounter.increment(increment);
        }
        
        // Update or create per-user gauge
        userGauges.computeIfAbsent(userId, uid -> 
            Gauge.builder("clicks.user_count", () -> perUserCounts.getOrDefault(uid, 0L))
                .description("Number of clicks per user")
                .tag("user_id", uid)
                .register(meterRegistry)
        );
    }

    public long getTotalClicks() {
        return totalClicks.get();
    }

    public Map<String, Long> getPerUserCounts() {
        return Map.copyOf(perUserCounts);
    }
}

