import json
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Glue job name (also set as env var in Lambda console)
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "process_reviews_job")

glue_client = boto3.client("glue")


def lambda_handler(event, context):
    """
    Triggered by S3 when a new file is uploaded to the landing bucket.
    Starts the Glue ETL job.
    """
    logger.info("Received event: %s", json.dumps(event))

    # Try to log which file triggered the event (for debugging)
    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        logger.info("New object created: s3://%s/%s", bucket_name, object_key)
    except Exception as e:
        logger.warning("Could not parse S3 event details: %s", str(e))
        bucket_name = None
        object_key = None

    # Start the Glue job
    try:
        # If you want, you can pass bucket/key as Arguments to the Glue script
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            # Arguments={
            #     "--landing_bucket": bucket_name,
            #     "--object_key": object_key
            # }
        )

        job_run_id = response["JobRunId"]
        logger.info("Started Glue job %s with run ID %s", GLUE_JOB_NAME, job_run_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Glue job started",
                "glueJobName": GLUE_JOB_NAME,
                "jobRunId": job_run_id
            })
        }

    except Exception as e:
        logger.error("Error starting Glue job: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Failed to start Glue job",
                "error": str(e)
            })
        }
