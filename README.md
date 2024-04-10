# Open Table formats MLOps benchmark

## Setup Instructions
This section details the setup process required to run benchmarks on a Spark cluster, utilizing Docker Compose for the cluster setup, Poetry for Python dependency management, and pyenv for managing the Python version.

### Prerequisites
Docker and Docker Compose
Python 3.11 (If not available, install via pyenv)
pyenv (Optional, for managing multiple Python versions)
Poetry for dependency management

### Step 1: Install Python 3.11 Using pyenv
If Python 3.11 is not installed on your system, you can use pyenv to install and manage multiple Python versions.

1. Install pyenv (skip if already installed):
Follow the instructions on the pyenv GitHub page.

2. Install Python 3.11:
```
pyenv install 3.11.0
pyenv global 3.11.0
```

3. Verify Installation:
```
python --version
```
This should output Python 3.11.0 or the latest 3.11.x version you installed.

### Step 2: Setup Your Project Using Poetry
1. Install Poetry (skip if already installed):
```
curl -sSL https://install.python-poetry.org | python3 -
```
2. Navigate to Your Project Directory:
```
poetry install
```

### Step 3: Docker Compose for Spark Cluster
Start the Spark Cluster:
```
docker-compose up -d
```

### Step 4: Running Benchmarks
Run Your Benchmark Script:
Use the following command to run your benchmarks, replacing [type] with the specific benchmark type you wish to execute:
```
poetry run python benchmarking/run.py --spark_master [spark-master] --benchmark_type [type] --format [format]
```

Benchmark type:
- time-travel
- query-efficiency

Additional workload supporting types:
- initial-ingestion
- ingestion

Format:
- delta
- iceberg
- hudi
