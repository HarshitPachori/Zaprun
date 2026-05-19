package com.zaprun.common.event;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record JobEvent(
    UUID jobId,
    UUID executionId,
    UUID tenantId,
    String targetUrl,
    String httpMethod,
    Map<String, String> headers,
    String payload,
    int attempt,
    int maxRetries,
    int timeoutSeconds,
    String idempotencyKey,
    Instant scheduledAt) {}
