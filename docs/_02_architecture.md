# Architecture Documentation

## High-Level Architecture

Customer Activity
↓
Python Producer
↓
Kafka Topic
↓
Spark Structured Streaming
↓
PostgreSQL
↓
Analytics / AI

---

## Components

### Producer

Purpose:

Generate customer events.

Example:

{
"user_id": 101,
"event_type": "view_product",
"product_id": 500,
"timestamp": 1712000000
}

Output Topic:

customer_events

---

### Kafka

Purpose:

Message broker between producer and consumer.

Benefits:

* Buffering
* Decoupling
* Scalability
* Fault tolerance

Current Topics:

customer_events

Future Topics:

customer_events_raw
customer_events_enriched
customer_metrics

---

### Spark Structured Streaming

Purpose:

Consume Kafka messages continuously.

Responsibilities:

* Schema enforcement
* Data validation
* Data enrichment
* Aggregations
* Window calculations

---

### PostgreSQL

Purpose:

Store processed data.

Example Tables:

customer_events_processed
customer_metrics

---

## Data Flow

Step 1

Producer creates event

Step 2

Kafka stores event

Step 3

Spark reads event

Step 4

Schema applied

Step 5

Transformation applied

Step 6

Aggregation calculated

Step 7

Results written to PostgreSQL
