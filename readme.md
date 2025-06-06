## Project Description

The Road Incident Analytics System processes live road incident data from DriveBC, leveraging Spark Streaming, AWS (Kinesis, Lambda, EMR, S3, Athena, QuickSight), and SparkML to predict and visualize accident risks. The system ingests active incidents from an API, processes and stores data in S3 in parquet format, trains an ML model using historical data, and visualizes spatial clusters in QuickSight with the help of DBScan.

## Technologies

- DriveBC API
- Python
- SQL
- Apache Spark
- Kinesis Data Streams
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
