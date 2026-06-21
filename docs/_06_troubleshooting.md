# Troubleshooting Guide

## Producer Cannot Connect

Error:

Connection Refused

Check:

docker logs kafka

Verify:

KAFKA_ADVERTISED_LISTENERS

---

## Spark Job Fails

Check:

docker logs spark

Common Causes:

* Missing package
* Network issue
* Schema issue

---

## Kafka Topic Missing

Verify:

kafka-topics --list

Create topic if required.

---

## PostgreSQL Unreachable

Verify:

docker ps

Verify:

postgres container status

Verify:

database credentials

---

## Permission Denied

Check:

ls -la

Fix:

chmod
chown

---

## Docker Network Issue

Check:

docker network ls

Inspect:

docker network inspect

---

## Container Crash Loop

Check:

docker logs container_name

Identify startup error.
