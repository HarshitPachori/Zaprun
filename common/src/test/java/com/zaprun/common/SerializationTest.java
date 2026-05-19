package com.zaprun.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaprun.common.event.JobEvent;
import com.zaprun.common.failure.FailureType;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SerializationTest {
  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  @Test
  void jobEvent_serialiesAndDeserializes() throws Exception {
    var event =
        new JobEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "https://example.com/webhook",
            "POST",
            Map.of("Content-Type", "application/json"),
            "{\"key\":\"value\"}",
            1,
            3,
            30,
            "idem-key-123",
            Instant.now());
    var json = mapper.writeValueAsString(event);
    var result = mapper.readValue(json, JobEvent.class);
    assertEquals(event.jobId(), result.jobId());
    assertEquals(event.targetUrl(), result.targetUrl());
  }

  @Test
  void failureType_networkTimeout_serializesWithTypeInfo() throws Exception {
    FailureType failure = new FailureType.NetworkTimeout("connection refused");
    var json = mapper.writeValueAsString(failure);
    assertTrue(json.contains("NETWORK_TIMEOUT"));
    var result = mapper.readValue(json, FailureType.class);
    assertInstanceOf(FailureType.NetworkTimeout.class, result);
  }

  @Test
  void failureType_httpClientError_roundTrips() throws Exception {
    FailureType failure = new FailureType.HttpClientError(400, "bad request");
    var json = mapper.writeValueAsString(failure);
    var result = mapper.readValue(json, FailureType.class);
    assertInstanceOf(FailureType.HttpClientError.class, result);
    assertEquals(400, ((FailureType.HttpClientError) result).statusCode());
  }
}
