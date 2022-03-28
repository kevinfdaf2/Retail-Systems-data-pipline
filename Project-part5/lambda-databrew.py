import boto3
databrew= boto3.client('databrew')
def lambda_handler(event, context):
    response = databrew.start_job_run(Name='user-features-2-job')
    response1 = databrew.start_job_run(Name='up-features-job')
    response2 = databrew.start_job_run(Name='user-features-1-job')
    response3 = databrew.start_job_run(Name='prd-features-job')
