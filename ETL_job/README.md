# STEDI Human Balance Analytics

## Project Overview
The STEDI Human Balance Analytics project involves building an ETL pipeline using AWS Glue Studio to process data from IoT devices, customer information, and accelerometer readings. The data is ingested from the Landing Zone, transformed in the Trusted Zone, and finally curated for machine learning analysis.

---

## Data Flow and ETL Process

### Landing Zone
1. **customer_landing_to_trusted.py**: Filters out rows where `shareWithResearchAsOfDate` is blank.
2. **accelerometer_landing_to_trusted.py**: Joins `customer_trusted` with `accelerometer_landing` by `email`.
3. **step_trainer_trusted.py**: Processes trusted data from step trainer records.

### Athena Query Results
**Landing Zone Query Results:**
- `customer_landing`: 956 rows  
  ![customer_landing Query](./screenshots_images/customer_landing.png)

- `accelerometer_landing`: 81,273 rows  
  ![accelerometer_landing Query](./screenshots_images/accelerometer_landing.png)

- `step_trainer_landing`: 28,680 rows  
  ![step_trainer_landing Query](./screenshots_images/step_trainer_landing.png)

### Trusted Zone
**Query Results:**
- `customer_trusted`: 482 rows (No blank `shareWithResearchAsOfDate`)  
  ![customer_trusted Query](./screenshots_images/customer_trusted.png)

- `accelerometer_trusted`: 40,981 rows  
  ![accelerometer_trusted Query](./screenshots_images/Accelerometer_Landing_to_Trusted.png)

- `step_trainer_trusted`: 14,460 rows  
  ![step_trainer_trusted Query](./screenshots_images/step_trainer_trusted.png)

---

## Curated Zone
1. **customer_trusted_to_curated.py**: Joins `customer_trusted` with `accelerometer_trusted` by `email`.
2. **machine_learning_curated.py**: Joins `step_trainer_trusted` with `accelerometer_trusted` by `sensorReadingTime`.

**Query Results:**
- `customer_curated`: 482 rows  
  ![customer_curated Query](./screenshots_images/customers_curated.png)

- `machine_learning_curated`: 43,681 rows  
  ![machine_learning_curated Query](./screenshots_images/machine_learning_curated.png)

---

## Tools Used
- **AWS Glue Studio**: ETL pipeline development  
- **AWS Athena**: Query and analyze data  
- **AWS S3**: Data storage  
- **AWS Glue Data Catalog**: Metadata management  

---

## Screenshots Summary
1. AWS Glue Job Configurations  
2. Athena Query Results  
3. Data Preview Sessions from Glue Studio  
