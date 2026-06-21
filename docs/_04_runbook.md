# Operations Runbook

## Start Environment

docker compose up -d

---

## Stop Environment

docker compose down

---

## View Running Containers

docker ps

---

## View All Containers

docker ps -a

---

## Kafka Logs

docker logs kafka

---

## Spark Logs

docker logs spark

---

## PostgreSQL Logs

docker logs postgres

---

## Create Topic

docker exec kafka kafka-topics 
--create 
--topic customer_events 
--bootstrap-server kafka:9092

---

## List Topics

docker exec kafka kafka-topics 
--list 
--bootstrap-server kafka:9092

---

## Describe Topic

docker exec kafka kafka-topics 
--describe 
--topic customer_events 
--bootstrap-server kafka:9092

---

## Restart Kafka

docker restart kafka

---

## Restart Spark

docker restart spark

---

## Restart PostgreSQL

docker restart postgres

---

## Verify PostgreSQL

docker exec -it postgres psql -U postgres

---

## Startup Sequence

1. Zookeeper
2. Kafka
3. PostgreSQL
4. Spark
5. Producer

---

## Shutdown Sequence

1. Producer
2. Spark
3. Kafka
4. PostgreSQL
5. Zookeeper
