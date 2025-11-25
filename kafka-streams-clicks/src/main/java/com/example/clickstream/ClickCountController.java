package com.example.clickstream;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClickCountController {

    private final ClickCountService clickCountService;

    public ClickCountController(ClickCountService clickCountService) {
        this.clickCountService = clickCountService;
    }

    @GetMapping("/clicks/count")
    public ClickCountResponse getTotalClicks() {
        return new ClickCountResponse(
            clickCountService.getTotalClicks(),
            clickCountService.getPerUserCounts()
        );
    }
}
