# Real-Time GitHub Event Monitoring System


## Project Overview
In this project, we designed and implemented a distributed, real-time event monitoring system. The system ingests streaming data from the GitHub Archive, processes various types of developer activities (e.g., PushEvent, WatchEvent, CreateEvent) in real-time, and visualizes the metrics on a professional dashboard.

Our primary goal is to simulate realistic streaming behavior by chunking historical data and processing it continuously, ensuring zero data loss and fault tolerance through a decoupled architecture.

## Architecture & Technology Stack
We utilized a modern, industry-standard Big Data pipeline:
* **Data Source:** GitHub Archive (JSON format).
* **Ingestion (Producer):** Python script simulating real-time streaming by chunking data and adding artificial delays.
* **Message Broker:** Apache Kafka (running in KRaft mode) for robust, fault-tolerant data queuing.
* **Stream Processing (Consumer):** Python-based consumer for real-time event aggregation and counting.
* **Time-Series Database:** InfluxDB for high-performance writing and storage of time-series metrics.
* **Visualization:** Grafana for real-time, dynamic dashboard rendering.
* **Infrastructure as Code (IaC):** Docker & Docker Compose with **Grafana Provisioning** for fully automated, zero-touch reproducible environments.

## Key Features
* **Realistic Streaming Simulation:** Data is not dumped at once; it is sent in micro-batches with controlled delays to mimic live production traffic.
* **Decoupled & Fault-Tolerant:** By utilizing Apache Kafka, the ingestion and processing layers are completely separated. If the consumer fails, Kafka retains the offset (backlog), allowing the system to resume processing without any data loss.
* **Automated Professional Dashboard:** A dynamic Grafana dashboard featuring time-series flow, event distribution, and real-time processing gauges, configured automatically on startup via provisioning.

## Getting Started

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* Python 3.12 installed.
* A sample data file from [GitHub Archive](https://www.gharchive.org/) placed in the `data/` directory (e.g., `2026-04-16-12.json.gz`).

### 1. Start the Infrastructure
Start the Kafka broker, InfluxDB, and Grafana using Docker Compose:
```bash
docker-compose up -d
