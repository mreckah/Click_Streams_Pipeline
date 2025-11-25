package com.example.clickstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClickCountStream {

    @Bean
    public KTable<String, Long> clickCountTable(StreamsBuilder builder,
                                                @Value("${kafka.topics.clicks}") String clicksTopic,
                                                @Value("${kafka.topics.clickCounts}") String clickCountsTopic) {
        KTable<String, Long> clickCounts = builder
            .stream(clicksTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as("click-counts-store"));

        clickCounts
            .toStream()
            .to(clickCountsTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return clickCounts;
    }
}
