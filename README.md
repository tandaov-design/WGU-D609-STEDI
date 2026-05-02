# WGU D609 - STEDI Human Balance Analytics

## Project Overview
This project implements a data lakehouse solution for the STEDI Human Balance Analytics 
use case using AWS services. The goal is to curate IoT sensor data and mobile application 
data so that it can be used by data scientists to train a machine learning model for step 
detection.

## Architecture
The pipeline is organized into three data zones:

- **Landing Zone** — Raw JSON data stored in Amazon S3, queried with Athena
- **Trusted Zone** — Filtered to only include customers who consented to research
- **Curated Zone** — Joined and refined data ready for machine learning

## AWS Services Used
- Amazon S3
- AWS Glue Studio
- Apache Spark
- Amazon Athena
- AWS Glue Data Catalog

## Glue Jobs
| Script | Description |
|---|---|
| customer_landing_to_trusted.py | Filters customers who consented to share research data |
| accelerometer_landing_to_trusted.py | Filters accelerometer data to consented customers only |
| customer_trusted_to_curated.py | Creates curated customers with valid accelerometer data |
| step_trainer_trusted.py | Filters step trainer data to curated customers by serial number |
| machine_learning_curated.py | Joins step trainer and accelerometer data by timestamp for ML |

## Row Count Validation
| Table | Expected Rows |
|---|---|
| customer_landing | 956 |
| accelerometer_landing | 81,273 |
| step_trainer_landing | 28,680 |
| customer_trusted | 482 |
| accelerometer_trusted | 40,981 |
| step_trainer_trusted | 14,460 |
| customer_curated | 482 |
| machine_learning_curated | 43,681 |

## Files Included
- `customer_landing.sql` — DDL for customer landing table
- `accelerometer_landing.sql` — DDL for accelerometer landing table
- `step_trainer_landing.sql` — DDL for step trainer landing table
- `customer_landing_to_trusted.py` — Glue job script
- `accelerometer_landing_to_trusted.py` — Glue job script
- `customer_trusted_to_curated.py` — Glue job script
- `step_trainer_trusted.py` — Glue job script
- `machine_learning_curated.py` — Glue job script
- Screenshots of all Athena query results for validation
