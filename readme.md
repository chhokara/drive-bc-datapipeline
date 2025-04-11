## Project Description

The Real-Time Road Incident Analytics System processes live road incident data from DriveBC, leveraging Kafka, Spark Streaming, AWS (S3, Athena, QuickSight, SageMaker), and Machine Learning to predict and visualize accident risks. The system ingests real-time incidents, processes and stores data in S3 in parquet format, trains an ML model on SageMaker to classify accidents, and visualizes trends in QuickSight.

## Technologies

- DriveBC API
- Python
- SQL
- Apache Spark
- Kinesis Data Stream
- Lambda
- EMR
- S3
- Athena
- QuickSight
- DBScan

## Data Pipeline

![Pipeline Overview](assets/pipeline.png)

## Dashboard
![Dashboard](assets/dashboard.drawio.png)
