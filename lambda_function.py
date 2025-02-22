import json
import boto3
import os

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    Job_Name = os.environ.get('GLUE_JOB_NAME','Incremental_load_to_redshift')

    
    try:
        # trigger the glue job to run
        response = glue_client.start_job_run(JobName = Job_Name)
        print('Glue Job Started Successfully:', response)

        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue JOb '{Job_Name}' triggered successfully!")
        }
    except Exception as e:
        print("Got Error During Triggering Glue Job:")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error triggering Glue job: {str(e)}")
        }
