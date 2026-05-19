package com.zaprun.common.failure;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = FailureType.NetworkTimeout.class, name = "NETWORK_TIMEOUT"),
  @JsonSubTypes.Type(value = FailureType.HttpClientError.class, name = "HTTP_CLIENT_ERROR"),
  @JsonSubTypes.Type(value = FailureType.HttpServerError.class, name = "HTTP_SERVER_ERROR"),
  @JsonSubTypes.Type(value = FailureType.WorkerOOM.class, name = "WORKER_OOM"),
  @JsonSubTypes.Type(value = FailureType.BusinessLogicError.class, name = "BUSINESS_LOGIC_ERROR"),
  @JsonSubTypes.Type(value = FailureType.Unknown.class, name = "UNKNOWN")
})
public sealed interface FailureType
    permits FailureType.NetworkTimeout,
        FailureType.HttpClientError,
        FailureType.HttpServerError,
        FailureType.WorkerOOM,
        FailureType.BusinessLogicError,
        FailureType.Unknown {

  record NetworkTimeout(String message) implements FailureType {}

  record HttpClientError(int statusCode, String body) implements FailureType {}

  record HttpServerError(int statusCode, String body) implements FailureType {}

  record WorkerOOM(String message) implements FailureType {}

  record BusinessLogicError(String message) implements FailureType {}

  record Unknown(String message) implements FailureType {}
}
