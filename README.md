# Serverless Spark ETL Pipeline on AWS (Product Reviews)

This repo contains my solution for **ITCS 6190/8190 – Hands-on L13: Serverless Spark ETL Pipeline on AWS**.  
It implements an event-driven pipeline on AWS: when a new product review CSV is uploaded to S3, a Lambda function starts a Glue Spark ETL job that cleans the data, runs analytics, and writes Parquet results to a processed S3 bucket. :contentReference[oaicite:1]{index=1}

---

## 🏗 Architecture

**Flow:** `S3 (landing) → Lambda → AWS Glue (Spark ETL) → S3 (processed results)`

Components:

- **S3**  
  - Landing bucket: `handsonfinallanding`  
  - Processed bucket: `handsonfinalprocessed`
- **AWS Lambda**
  - Function: `start_glue_job_trigger`
- **AWS Glue**
  - Job: `process_reviews_job`
  - Script: [`src/glue_etl_script.py`](src/glue_etl_script.py)

---

## ⚙️ How to Run

1. Upload `reviews.csv` to the landing bucket.
2. S3 event triggers Lambda.
3. Lambda starts the Glue job.
4. Glue:
   - Reads & cleans the CSV.
   - Runs 6 Spark SQL queries (3 base + 3 extra).
   - Writes Parquet outputs to the processed bucket.

See screenshots in the sections below for proof of execution.

---

## 📊 Extra Spark Queries Implemented

1. **Top 10 products by average rating** (min 5 reviews)  
2. **Daily count of low-rated reviews** (star_rating ≤ 2)  
3. **Customer rating stats** for customers with ≥ 3 reviews  

Each query writes its results as Parquet to a subfolder under `Athena Results/` in the processed bucket.

---

## 📸 Screenshots


```markdown
![S3 Buckets](screenshots/L13_S3.PNG )
![Glue Job Runs](screenshots/glue_job_run.png)
![Processed Outputs](screenshots/processed_results.png)
