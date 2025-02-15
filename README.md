# STEDI Data Pipeline Project

In this project, I built a data pipeline to process STEDI Step Trainer and mobile app data using AWS Glue, S3, and Athena. 

---

## Project Structure

```
stedi-data-pipeline/
├── data/
│   ├── landing/
│   │   ├── customer/
│   │   ├── accelerometer/
│   │   └── step_trainer/
│   ├── trusted/
│   │   ├── customer_trusted.json
│   │   ├── accelerometer_trusted.json
│   │   └── step_trainer_trusted.json
│   └── curated/
│       ├── customer_curated.json
│       └── machine_learning_curated.json
├── glue_jobs/
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── step_trainer_trusted.py
│   ├── customer_trusted_to_curated.py
│   └── machine_learning_curated.py
├── screenshots/
│   ├── customer_landing.png
│   ├── accelerometer_landing.png
│   ├── step_trainer_landing.png
│   ├── customer_trusted.png
│   ├── accelerometer_trusted.png
│   ├── step_trainer_trusted.png
│   ├── customer_curated.png
│   └── machine_learning_curated.png
└── sql_scripts/
    ├── customer_landing.sql
    ├── accelerometer_landing.sql
    ├── step_trainer_landing.sql
    ├── customer_trusted.sql
    ├── accelerometer_trusted.sql
    ├── step_trainer_trusted.sql
    ├── customer_curated.sql
    └── machine_learning_curated.sql
```

1. **`data/`** holds the JSON data for landing, trusted, and curated zones.  
2. **`glue_jobs/`** has the Python scripts that simulate AWS Glue jobs for filtering/joining data.  
3. **`screenshots/`** contains images showing Athena queries and row counts.  
4. **`sql_scripts/`** includes SQL DDL for creating Athena/Glue tables.

---

## Steps

### 1. Landing Zone
- Raw data (JSON) goes to S3:
  - **Customer**: 956 rows
  - **Accelerometer**: 81273 rows
  - **Step Trainer**: 28680 rows
- Created three tables in Athena using:
  - `customer_landing.sql`
  - `accelerometer_landing.sql`
  - `step_trainer_landing.sql`
- Confirmed row counts in Athena.

### 2. Trusted Zone
- **Glue Jobs (or local PySpark)**:
  1. `customer_landing_to_trusted.py` filters customers by `shareWithResearchAsOfDate`.
  2. `accelerometer_landing_to_trusted.py` joins with `customer_trusted` to keep only consenting users.
  3. `step_trainer_trusted.py` filters or joins step trainer data for consenting users.
- Row counts:
  - `customer_trusted` = 482
  - `accelerometer_trusted` = 40981
  - `step_trainer_trusted` = 14460

### 3. Curated Zone
- **Additional Glue Jobs**:
  1. `customer_trusted_to_curated.py` = join `customer_trusted` + `accelerometer_trusted` → `customer_curated.json` (482 rows).
  2. `machine_learning_curated.py` = join `step_trainer_trusted` + `accelerometer_trusted` → `machine_learning_curated.json` (43681 rows).
- Created final Athena tables using:
  - `customer_curated.sql`
  - `machine_learning_curated.sql`
- Verified row counts in Athena.

---

## Screenshots

All screenshots are in the `screenshots/` folder:
1. **Landing**: 
   - `customer_landing.png` (956 rows)
   - `accelerometer_landing.png` (81273 rows)
   - `step_trainer_landing.png` (28680 rows)
2. **Trusted**:
   - `customer_trusted.png` (482 rows)
   - `accelerometer_trusted.png` (40981 rows)
   - `step_trainer_trusted.png` (14460 rows)
3. **Curated**:
   - `customer_curated.png` (482 rows)
   - `machine_learning_curated.png` (43681 rows)

---

## Conclusion

- I used Glue scripts to filter data into **Trusted** and **Curated** zones.
- I created Athena tables for each stage using the SQL scripts.
- The final row counts match the project requirements. 
