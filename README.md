# Cross-Protocol Risk Dashboard & Alert Service

This project ingests live Aave and Compound events, computes real-time health factors, P&L deltas, rolling VaR (Value-at-Risk) & LaR (Liquidations-at-Risk), and warehouses both raw and aggregated data in Parquet + DuckDB/dbt.


## Prerequisites

1. **Docker & Docker Compose**  
2. **Python 3.10+**  
3. **Virtualenv** (or venv)  
4. **Windows** (PowerShell/CMD) or Mac/Linux shell  

---

## Setup

### 1. **Clone repo**  
```
    bash
    git clone https://github.com/your-username/risk-dashboard.git
    cd risk-dashboard
```


### 2. Start Kafka & ZooKeeper
```
    cd docker
    docker compose up -d
    docker compose ps   # verify kafka:9092 & zookeeper:2181 are Up
    cd ..
```


### 3. Create & activate Python venv

```
    python -m venv venv
    .\venv\Scripts\Activate.ps1  
```



### 4. Create .env file
``` 
    ALCHEMY_WS_URL=wss://eth-mainnet.g.alchemy.com/v2/XXXXXXXXXXXXXXXX
    SLACK_WEBHOOK=https://hooks.slack.com/services/…
    KAFKA_BROKERS=localhost:9092
    ETHERSCAN_KEY=API
    ALERT_HF=1.05
    ALERT_THROTTLE=3600
```


### 5. Running the Pipeline
#### 5.1. Producer: ingest raw events
#### In Terminal A
    
    python ingest/producer.py


#### 5.2. Consumer: compute HF & deltas
#### In Terminal B

    python -m pipeline.consumer


#### 5.3. Aggregator: compute VaR & LaR
#### In Terminal C

    python -m pipeline.aggregator



### 6. Warehouse Layer
#### 6.1. Snapshot script

    # Snapshot deltas
    $Env:SNAPSHOT_TOPIC="risk-deltas"    # PowerShell
    python pipeline/snapshot.py

    # Snapshot metrics
    $Env:SNAPSHOT_TOPIC="risk-metrics"
    python pipeline/snapshot.py

    # Verify
    dir snapshots\*.parquet


#### 6.2. dbt models

    cd risk_dw
    # build + test
    dbt deps
    dbt run
    dbt test
    # docs
    dbt docs generate
    dbt docs serve --port 8888   # visit http://localhost:8888




## Restarting from Scratch
Whenever you need to bring everything back up (without wiping files):
Stop all Python processes (Ctrl-C) and Docker containers:
```
    cd docker
    docker compose down
```
```
    cd docker
    docker compose up -d
    cd ..
    .\venv\Scripts\Activate.ps1   # or activate venv
```
Run producer, consumer, aggregator, then snapshot + dbt in separate terminals as above.