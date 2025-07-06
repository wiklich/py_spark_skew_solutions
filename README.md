# Spark Skew Join and Other Skews Solution in PySpark

This project demonstrates and solves real-world **Spark Skew Join** and "**Others Skew**" problems using salting techniques and other optimization strategies in PySpark. It includes tools for generating skewed data, detecting skew patterns, and running demonstrations to compare performance with and without mitigation strategies.

---

## 📚 Table of Contents

- [Overview](#overview)
- [Scenarios of Data Skew in Spark](#scenarios-of-data-skew-in-spark)
- [Purpose of `build_zip.py`](#purpose-of-build_zippy)
- [Docker Setup](#docker-setup)
- [CLI Commands](#cli-commands)
  - [Generate Skewed Data](#generate-skewed-data)
  - [Detect Skew](#detect-skew)
  - [Run Demonstration Jobs](#run-demonstration-jobs)

---

## Overview

This repository provides a structured way to understand and address **data skew** issues that commonly occur during Spark joins and aggregations. It includes:

- Scripts to generate various types of data skew.
- Tools to detect skew in datasets.
- Demonstrations comparing job execution with and without skew mitigation (e.g., salting, AQE).

---

## Scenarios of Data Skew in Spark

| ID | Scenario Name                             | Description                                                                 | Skew Type         | Practical Example                                               |
|----|-------------------------------------------|-----------------------------------------------------------------------------|-------------------|-----------------------------------------------------------------|
| A  | Many keys with few records                | High number of distinct keys, each with small amount of data               | Task scheduling   | Logs with unique `session_id` per visit                         |
| B  | Few keys with many records                | One or few keys dominate the dataset                                        | Classic skew      | Sales by `product_id`, where one product dominates              |
| C  | Combination of A + B                      | Mix of many small keys and a few dominant ones                              | Mixed skew        | App with mostly inactive users and some super-users             |
| D1 | High cardinality with unequal distribution| Many keys, but some are disproportionately frequent                       | Hidden skew       | `customer_id` with millions of clients, but few very active     |
| D2 | Composite key with skew in combinations | Specific combinations of columns concentrate most data                      | Compound skew     | `(country, product)` where `"BR", "X"` dominates                |
| D3 | Temporal data skew                        | Certain time periods have significantly higher volume                       | Seasonal skew     | Logs with traffic spikes at peak hours                          |
| D7 | Skew in join operations                   | Most transactions relate to a few `customer_id`, causing uneven task load  | Join skew         | Transactions joined with customers, 75% concentrated on 10 users|

---

## Purpose of `build_zip.py`

The `scripts/build_zip.py` script prepares your code for deployment, especially when submitting jobs to a Spark cluster.

### What it does:
1. Packages all modules from `src/py_spark_skew_solutions` into a `.zip` file.
2. Copies the main entrypoint (`jobs/job_spark_skew_solutions.py`) to the `dist/` directory.

### Resulting structure after running:
```
dist/
├── skew_solutions_lib.zip       # Contains all Python modules
└── job_spark_skew_solutions.py  # Main script used by spark-submit
```

### Submitting a job:
```bash
spark-submit   --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip   /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py   generate_skew_data --help
```

---

## Docker Setup

Use your own `docker-compose.yml` to simulate a full Spark cluster.

### Build and Start the Environment
```bash
docker-compose up -d
```

Wait until all services are ready.

### Prepare Your Code for Submission
Run the build script to create the `.zip` package:
```bash
python scripts/build_zip.py
```

### Copy Files to Container
```bash
docker cp dist/skew_solutions_lib.zip spark-master:/opt/bitnami/spark/jobs/app/skew_solutions_lib.zip
docker cp dist/job_spark_skew_solutions.py spark-master:/opt/bitnami/spark/jobs/app/
```

---

## CLI Commands

This project uses [Typer](https://typer.tiangolo.com/) to provide a powerful and auto-documented CLI interface. Use the built-in `--help` option to explore all available commands and parameters.

### Show Help
```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  --help
```

---

### Generate Skewed Data

#### Examples:
```bash

# Generate Skew Data Help
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data --help

# Generate scenario A
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data A \
  -o /opt/bitnami/spark/jobs/app/skew_data/scenario_a

# Generate scenario B
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data B \
  -o /opt/bitnami/spark/jobs/app/skew_data/scenario_b

# Generate scenario C
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data C \
  -o /opt/bitnami/spark/jobs/app/skew_data/scenario_c

# Generate scenario D1
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data D1 \
  -o /opt/bitnami/spark/jobs/app/skew_data/scenario_d1

# Generate scenario D2
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data D2 \
  -o /opt/bitnami/spark/jobs/app/skew_data/scenario_d2

 # Generate scenario D3
 docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data D3 \
  -o /opt/bitnami/spark/jobs/app/skew_data/scenario_d3   

# Generate scenario D7 (join skew case)
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  generate_skew_data D7 \
  -ot /opt/bitnami/spark/jobs/app/skew_data/transactions \
  -oc /opt/bitnami/spark/jobs/app/skew_data/customers
```

---

### Detect Skew

#### Example:
```bash

# Scenario Detector Helper:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  detect_skew_data --help

# Detect skew in scenario A
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  detect_skew_data A \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_a

# Detect skew in scenario B
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  detect_skew_data B \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_b

# Detect skew in scenario C
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  detect_skew_data C \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_c

# Detect skew in scenario D1
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
/opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
detect_skew_data D1 \
-i /opt/bitnami/spark/jobs/app/skew_data/scenario_d1

# Detect skew in scenario D2
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
/opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
detect_skew_data D2 \
-i /opt/bitnami/spark/jobs/app/skew_data/scenario_d2

# Detect skew in scenario D3
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
/opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
detect_skew_data D3 \
-i /opt/bitnami/spark/jobs/app/skew_data/scenario_d3

# Detect skew in scenario D7
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
/opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
detect_skew_data D7 \
-i /opt/bitnami/spark/jobs/app/skew_data/transactions   

```

---

### Run Demonstration Jobs

Demonstrates the impact of skew and how to mitigate it using salting.

```bash

# Scenario Run Demonstration Help:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo --help

# Scenario A Run Demonstration

### With Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo A \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_a \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_a \
  -ds

### Solving Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo A \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_a \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_a \
  -nds \
  -aqe \
  -sp 6

# Scenario B Run Demonstration:
### With Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo B \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_b \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_b \
  -ds

### Solving Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=2 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo B \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_b \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_b \
  -nds \
  -no-aqe \
  -sp 6

# Scenario C Run Demonstration:

### With Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo C \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_c \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_c \
  -ds

### Solving Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=2 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo C \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_c \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_c \
  -nds \
  -no-aqe \
  -sp 6

# Scenario D1 Run Demonstration:

### With Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D1 \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_d1 \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d1 \
  -ds

### Solving Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=2 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D1 \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_d1 \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d1 \
  -nds \
  -no-aqe \
  -sp 6  

# Scenario D2 Run Demonstration:

### With Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D2 \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_d2 \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d2 \
  -ds

### Solving Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=2 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D2 \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_d2 \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d2 \
  -nds \
  -no-aqe \
  -sp 6

# Scenario D3 Run Demonstration:

### With Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D3 \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_d3 \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d3 \
  -ds

### Solving Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=2 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D3 \
  -i /opt/bitnami/spark/jobs/app/skew_data/scenario_d3 \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d3 \
  -nds \
  -no-aqe \
  -sp 6

# Scenario D7 Run Demonstration:

### With Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D7 \
  -it /opt/bitnami/spark/jobs/app/skew_data/transactions \
  -ic /opt/bitnami/spark/jobs/app/skew_data/customers \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d7 \
  -ds

### Solving Skew
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=2 \
  --py-files /opt/bitnami/spark/jobs/app/skew_solutions_lib.zip \
  /opt/bitnami/spark/jobs/app/job_spark_skew_solutions.py \
  run_demo D7 \
  -it /opt/bitnami/spark/jobs/app/skew_data/transactions \
  -ic /opt/bitnami/spark/jobs/app/skew_data/customers \
  -o /opt/bitnami/spark/jobs/app/skew_data_demonstration/scenario_d7 \
  -nds \
  -no-aqe \
  -sp 6        
```
