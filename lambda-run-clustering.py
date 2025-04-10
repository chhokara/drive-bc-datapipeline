import json
import boto3

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    
    emr = boto3.client('emr')
    
    cluster_id = 'j-3JK48QIJ73PJH'

    step_response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'Run Spark Clustering Script',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                      'spark-submit',
                      '--jars',
                      's3://etl-tb/jars/hadoop-aws-3.3.4.jar,s3://etl-tb/jars/aws-java-sdk-bundle-1.12.262.jar',
                      's3://clustered-data/cluster.py',
                      's3a://etl-tb/output/part-*',
                      's3://clustered-data/coord-clusters',
                      's3://clustered-data/severity-clusters'
                    ]
                }
            }
        ]
    )

    step_id = step_response['StepIds'][0]
    print(f"Started EMR Step: {step_id}")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Started EMR Step: {step_id}")
    }
