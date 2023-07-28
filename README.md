# Summary
Using Airflow in combination with AWS Redshift, AWS S3, Python, and SQL to create an automated Data Pipeline for the Data Engineering Nano Degree.

# Description
The Goal of this Project is to do analytics for a Music Streaming Platform.
For that reason, the logs of the songs that were listened to by the customers are combined with the information of
the songs. That data is originally stored in an S3 Bucket
Through the Automated Data Pipeline in Airflow, this data should first be loaded from S3 to redshift. There
a star schema is applied to be able to optimally query the data for latter analysis. 
At the end of the Pipeline a data quality check is performed. 


The Created pipeline looks as follows:
![Alt text](dag.png?raw=true "Title")