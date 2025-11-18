import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# =====================================================================================
# Glue job entrypoint
# =====================================================================================

# Glue passes JOB_NAME automatically when you run the job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# =====================================================================================
# S3 CONFIGURATION
# - Using your bucket names: 1handsonfinallanding, 1handsonfinalprocessed
# =====================================================================================

# Raw CSV bucket (landing)
LANDING_BUCKET = "1handsonfinallanding"
LANDING_KEY = "reviews.csv"  # exact file name in landing bucket

# Processed / results bucket
PROCESSED_BUCKET = "1handsonfinalprocessed"

# Full paths
input_path = f"s3://{LANDING_BUCKET}/{LANDING_KEY}"
processed_data_path = f"s3://{PROCESSED_BUCKET}/processed-data"
results_base_path = f"s3://{PROCESSED_BUCKET}/Athena Results"

# =====================================================================================
# 1. READ & CLEAN THE DATA
# =====================================================================================

# Read CSV from S3 (header + infer schema)
reviews_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(input_path)
)

# At this point, your columns look like:
#   rating, customer_id, product_id, review_id, review_date
# We want to standardize and make sure we have:
#   star_rating, customer_id, product_id, review_date

# ----- Handle rating / star_rating column -----
# If the CSV has "rating", create "star_rating" from it.
if "rating" in reviews_df.columns and "star_rating" not in reviews_df.columns:
    reviews_df = reviews_df.withColumn("star_rating", F.col("rating").cast("int"))
elif "star_rating" in reviews_df.columns:
    reviews_df = reviews_df.withColumn("star_rating", F.col("star_rating").cast("int"))

# ----- Handle review_date column (ensure it's a date) -----
if "review_date" in reviews_df.columns:
    reviews_df = reviews_df.withColumn("review_date", F.to_date(F.col("review_date")))

# List required columns we expect to be non-null
required_cols = []
for col_name in ["product_id", "customer_id", "review_date", "star_rating"]:
    if col_name in reviews_df.columns:
        required_cols.append(col_name)

# Drop rows missing any of the required fields we actually have
if required_cols:
    reviews_df = reviews_df.dropna(subset=required_cols)

# Write cleaned full dataset to processed-data (Parquet)
(
    reviews_df
        .repartition(1)  # optional: fewer files
        .write
        .mode("overwrite")
        .parquet(processed_data_path)
)

# Register temp view for Spark SQL
reviews_df.createOrReplaceTempView("reviews")

# =====================================================================================
# 2. SPARK SQL ANALYTICS
#    - 3 "baseline" queries
#    - 3 "extra" queries (required by the assignment)
# =====================================================================================

# ---------------------------------------------------------------------
# Query 1 – Daily review counts & average rating
# ---------------------------------------------------------------------
daily_review_counts = spark.sql("""
    SELECT
        review_date,
        COUNT(*)           AS review_count,
        AVG(star_rating)   AS avg_rating
    FROM reviews
    GROUP BY review_date
    ORDER BY review_date
""")

# ---------------------------------------------------------------------
# Query 2 – Top 5 customers by number of reviews
# ---------------------------------------------------------------------
top_5_customers = spark.sql("""
    SELECT
        customer_id,
        COUNT(*)         AS num_reviews,
        AVG(star_rating) AS avg_rating
    FROM reviews
    GROUP BY customer_id
    ORDER BY num_reviews DESC, avg_rating DESC
    LIMIT 5
""")

# ---------------------------------------------------------------------
# Query 3 – Rating distribution (count per star value)
# ---------------------------------------------------------------------
rating_distribution = spark.sql("""
    SELECT
        star_rating,
        COUNT(*) AS rating_count
    FROM reviews
    GROUP BY star_rating
    ORDER BY star_rating
""")

# ==========================================================================
# EXTRA QUERIES (3 additional Spark SQL queries)
# ==========================================================================

# ---------------------------------------------------------------------
# Query 4 – Top 10 products by average rating (min 5 reviews)
# ---------------------------------------------------------------------
top_10_products = spark.sql("""
    SELECT
        product_id,
        COUNT(*)         AS num_reviews,
        AVG(star_rating) AS avg_rating
    FROM reviews
    GROUP BY product_id
    HAVING COUNT(*) >= 5
    ORDER BY avg_rating DESC, num_reviews DESC
    LIMIT 10
""")

# ---------------------------------------------------------------------
# Query 5 – Daily count of low-rated (<= 2 stars) reviews
# ---------------------------------------------------------------------
daily_negative_reviews = spark.sql("""
    SELECT
        review_date,
        COUNT(*) AS num_negative_reviews
    FROM reviews
    WHERE star_rating <= 2
    GROUP BY review_date
    ORDER BY review_date
""")

# ---------------------------------------------------------------------
# Query 6 – Customer rating stats (customers with >= 3 reviews)
# ---------------------------------------------------------------------
customer_rating_stats = spark.sql("""
    SELECT
        customer_id,
        COUNT(*)         AS num_reviews,
        AVG(star_rating) AS avg_rating
    FROM reviews
    GROUP BY customer_id
    HAVING COUNT(*) >= 3
    ORDER BY num_reviews DESC
""")

# =====================================================================================
# 3. WRITE ALL QUERY RESULTS TO S3 (PARQUET)
# =====================================================================================

(
    daily_review_counts
        .write
        .mode("overwrite")
        .parquet(f"{results_base_path}/daily_review_counts")
)

(
    top_5_customers
        .write
        .mode("overwrite")
        .parquet(f"{results_base_path}/top_5_customers")
)

(
    rating_distribution
        .write
        .mode("overwrite")
        .parquet(f"{results_base_path}/rating_distribution")
)

(
    top_10_products
        .write
        .mode("overwrite")
        .parquet(f"{results_base_path}/top_10_products")
)

(
    daily_negative_reviews
        .write
        .mode("overwrite")
        .parquet(f"{results_base_path}/daily_negative_reviews")
)

(
    customer_rating_stats
        .write
        .mode("overwrite")
        .parquet(f"{results_base_path}/customer_rating_stats")
)

# =====================================================================================
# 4. COMMIT GLUE JOB
# =====================================================================================

job.commit()
