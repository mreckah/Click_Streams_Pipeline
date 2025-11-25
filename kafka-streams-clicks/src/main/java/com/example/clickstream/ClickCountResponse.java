package com.example.clickstream;

import java.util.Map;

public record ClickCountResponse(long total, Map<String, Long> perUser) {
}

