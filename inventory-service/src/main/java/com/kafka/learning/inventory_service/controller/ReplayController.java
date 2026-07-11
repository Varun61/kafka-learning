package com.kafka.learning.inventory_service.controller;

import com.kafka.learning.inventory_service.entity.FailedMessage;
import com.kafka.learning.inventory_service.service.ReplayService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
@RequiredArgsConstructor
public class ReplayController {

    private final ReplayService replayService;

    @PostMapping("/admin/replay")
    public ResponseEntity<String> replayFailedMessages() {

        replayService.replayFailedMessage();

        return ResponseEntity.ok().build();
    }
}
