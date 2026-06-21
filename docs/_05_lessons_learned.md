# Lessons Learned

## Lesson 1

Problem:

Kafka producer connection refused.

Root Cause:

Incorrect advertised listeners.

Fix:

Configured:

KAFKA_ADVERTISED_LISTENERS

Prevention:

Always verify container DNS names.

---

## Lesson 2

Problem:

Spark dependency resolution failure.

Root Cause:

Kafka connector package missing.

Fix:

Add spark-sql-kafka package.

Prevention:

Document required dependencies.

---

## Lesson 3

Problem:

Permission denied reading files.

Root Cause:

Linux ownership mismatch.

Fix:

chmod
chown

Prevention:

Verify file ownership before execution.

---

## Lesson 4

Problem:

Topic not found.

Root Cause:

Topic not created.

Fix:

Create topic before producer startup.

---

## Lesson 5

Problem:

Project became difficult to maintain.

Root Cause:

Missing documentation.

Fix:

Introduce architecture docs, runbook and troubleshooting guides.

---

## Key Takeaway

Documentation is part of the system.

If the documentation is missing, the project is incomplete.
