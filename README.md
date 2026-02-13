# CityCycle: Real-Time Urban Mobility Network

**CityCycle** is a data engineering portfolio project that ingests real-time bike sharing data into a graph database to analyze urban mobility patterns.

## Architecture
Data flows from a simulated stream into a graph for analysis:
`Citibike (CSV) -> Kafka -> Neo4j -> Graph Algorithms (PageRank)`

## Tech Stack
- **Streaming**: Apache Kafka (Confluent)
- **Database**: Neo4j (Graph Data Science)
- **Language**: Python 3.10+
- **Infrastructure**: Docker Compose

## Quick Start
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start Infrastructure:
   ```bash
   docker-compose up -d
   ```
