
# Data Lakes with Pyspark and S3

### Udacity Data Engineer Nano Degree


## Introduction
The purpose of this specific project was to learn how to successfully ingest data from pyspark, create separate parquet files for each table of information we needed, and finally write them into S3 buckets to store them.
There are two data sources provided in this project. One is a list of random songs (ID's, the artists, name of the songs, locations, ETC). The other data source provides information surrounding the events of those specific songs (what actions did the users take on that specific song). Our fictional company "Sparkify", is looking for a way to store this information which is easily accessible to our data analysts, as well as having to not worry about how this data is going to be funneled into Pyspark to S3. 

## The ETL Pipeline

Udacity provided us with specific S3 buckets to access the two data sources. First, we needed to create a script to create all the necessary tables to move forward. From there we needed to ingest the data from S3 to the Pyspark cluster we created. Once the upload from S3 to Pyspark is complete, we simply take the data from the spark dataframes and slice them into appropriate tables we need. Once we have the individual frames, we can then parquet them into the file paths we need them to go into. 


## What you need.
You'll need a certain level of Python.3x installed and the *Python* libraries *configparser* , *pyspark.sql*, *pyspark.sql.functions* and . You will also need to setup your own instance of a Pyspark Cluster and S3 buckets in AWS. Once you have all this done you can simply configure the *dl.cfg* file to whatever settings you created on your Redshift cluster.


    [default]
    AWS_ACCESS_KEY_ID = {'Enter Access Key Here'}
    
    AWS_SECRET_ACCESS_KEY = {'Enter Secret Key Here'}


