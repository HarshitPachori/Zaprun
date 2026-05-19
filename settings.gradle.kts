rootProject.name = "zaprun"

include(
    "common",
    "api-service",
    "scheduler-service",
    "worker-service",
    "notification-service"
)

project(":api-service").projectDir = file("api_service")
project(":scheduler-service").projectDir = file("scheduler_service")
project(":worker-service").projectDir = file("worker_service")
project(":notification-service").projectDir = file("notification_service")