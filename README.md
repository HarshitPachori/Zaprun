# ZapRun

Multi-tenant distributed job scheduler with DAG dependencies, quota enforcement, and real-time execution streaming.

## Tech Stack
Java 21 · Spring Boot 4.0.6 · Kafka · gRPC · PostgreSQL · Redis

## Services
- `api-service` — REST API + WebSocket
- `scheduler-service` — Job polling + dispatch
- `worker-service` — Job execution
- `notification-service` — Alerts + webhooks

## Local Development
```bash
docker-compose up
./gradlew build
```

## Documentation
See [Design Document](docs/design.md)