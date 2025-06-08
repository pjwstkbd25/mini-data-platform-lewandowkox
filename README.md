[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/ano0EjUK)
# Mini Data Platform in Docker Containers

## 🌐 Objective

Develop a mini data platform using Docker containers that simulates a business process, ingests data into PostgreSQL, captures changes with Debezium, streams data through Kafka, processes it in Spark, and stores it in MinIO in Delta format.

---

## 🔄 Overview of Architecture

1. **Python Script** simulates a business process.
2. **PostgreSQL** stores the simulated data.
3. **Debezium** captures changes in PostgreSQL.
4. **Kafka** streams the change events.
5. **Spark Structured Streaming** processes events from Kafka.
6. **MinIO + Delta Lake** stores processed data in S3-compatible storage.

---

## 💡 Task-by-Task Summary & Setup

### Task 1: Simulating a Business Process with Python

**Deliverables:**

* Python script:

  * Generates synthetic data.
  * Dynamically creates PostgreSQL tables.
  * Inserts records into the database.
* PostgreSQL container in `docker-compose.yml`.

### Task 2: Connecting Debezium to Capture Changes in PostgreSQL

**Deliverables:**

* Debezium added to `docker-compose.yml`.
* PostgreSQL configured with:

  * `wal_level=logical`
  * Replication settings.
* JSON serialization used for streaming messages.

### Task 3: Kafka Setup & Streaming Events in JSON Format

**Deliverables:**

* Kafka + Zookeeper configured in Docker.
* JSON serialization set.
* Schema Registry (optional, used with AVRO).
* Kafka consumer for testing streaming pipeline.

### Task 4: Integrating Spark with Kafka for Data Processing

**Deliverables:**

* Spark container included.
* Spark job consuming messages from Kafka.
* Basic transformation logic applied (e.g., JSON parsing, selecting fields).

### Task 5: Storing Processed Data in MinIO using Delta Lake

**Deliverables:**

* MinIO configured and exposed on ports `9100` and `9101`.
* Spark job writing transformed data to Delta format in `s3a://minio-bucket/debezium-data`.
* ✍️ See `read_minio_delta.md` for how to retrieve data from MinIO using Spark.

### Task 6: Automating Deployment & Ensuring Reliability

**Deliverables:**

* All services defined in one `docker-compose.yml` file.
* Healthchecks and restarts added.
* Custom Docker network.
* One-command deployment:

```bash
docker-compose up --build
```

---

## 🚀 How to Deploy from Scratch

### 1. Clone the Project

```bash
git clone <repo-url>
cd <project-folder>
```

### 2. Run the Stack

```bash
docker-compose up --build
```

### 3. Optional: Initialize Kafka Connect

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d @connector-config.json
```

### 4. Access MinIO (S3 Browser)

* URL: [http://localhost:9101](http://localhost:9101)
* Username: `minioadmin`
* Password: `minioadmin`

### 5. Explore Data

* Stored in: `minio-bucket/debezium-data`
* Format: Delta (Parquet + transaction logs)

See [read\_minio\_delta.md](./read_minio_delta.md) for details.

### 6. Reset Environment

```bash
docker-compose down -v
```

---

## 📊 Tech Stack

* **PostgreSQL 15** with Debezium WAL
* **Debezium Connect 2.5**
* **Kafka + Zookeeper (Confluent Platform)**
* **Schema Registry (optional)**
* **Spark 3.5.0**
* **MinIO (S3-compatible)**
* **Delta Lake 3.0.0**

---

## 🔍 Related Docs

* [read\_minio\_delta.md](./read_minio_delta.md) — how to query stored Delta tables from MinIO with Spark.
* [init-connector.sh](./init-connector.sh) — script to register a Debezium connector.
