# Technical Design Document

## Why Kafka?

Without Kafka:

Producer → Spark

Problem:

If Spark is unavailable, data is lost.

With Kafka:

Producer → Kafka → Spark

Benefits:

* Event durability
* Replay capability
* Multiple consumers
* Scalability

---

## Why Spark?

Requirements:

* Streaming processing
* Window aggregations
* Scalable architecture

Spark provides:

* Structured Streaming
* Fault tolerance
* Distributed execution

---

## Why PostgreSQL?

Requirements:

* Store processed metrics
* SQL querying
* Easy integration

Benefits:

* Mature technology
* Easy local deployment
* Dashboard friendly

---

## Why Docker?

Requirements:

* Environment consistency
* Simplified deployment

Benefits:

* Isolated services
* Reproducible environment
* Easier onboarding

---

## Schema Design

Current Event Schema:

user_id
event_type
product_id
timestamp

Future Schema:

user_id
event_type
product_id
session_id
country
device_type
timestamp

---

## Partitioning Strategy

Current:

Single partition

Future:

Partition by:

* user_id
* event_type

Benefits:

* Parallel processing
* Better scalability

---

## Future Enhancements

* Airflow orchestration
* ML models
* Vector database
* Recommendation engine
* Monitoring stack
