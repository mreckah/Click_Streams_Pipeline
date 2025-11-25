package com.example.clickstream;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic clicksTopic(@Value("${kafka.topics.clicks}") String topicName) {
        return TopicBuilder.name(topicName)
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic clickCountsTopic(@Value("${kafka.topics.clickCounts}") String topicName) {
        return TopicBuilder.name(topicName)
            .partitions(3)
            .replicas(1)
            .build();
    }
}

