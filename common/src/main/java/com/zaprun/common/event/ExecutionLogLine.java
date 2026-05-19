package com.zaprun.common.event;

import com.zaprun.common.enums.LogLevel;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record ExecutionLogLine(
    UUID executionId,
    UUID tenantId,
    Instant timestamp,
    LogLevel level,
    String message,
    Map<String, String> metadata) {}
