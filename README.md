# DENDDataLakeProject
 

### Table of Contents

1. [Project Overview](#summary)
2. [File Structure](#Files)
3. [Data Schema](#schema)
4. [Instructions to run the files](#inst)
5. [Acknowledgements](#licensing)


    
## Project Overview<a name="summary"></a>

This project is part of Data Engineering Nano degree - DataLake, which mainly focuses on a music streaming startup, Sparkify, 
that want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## File Structure<a name="Files"></a>

1. data: data files are added for practice reasons all sparkify data reside in s3 paths: s3://udacity-dend/song_data/ & s3://udacity-dend/log_data/
2. dl.cfg: holds our AWS user info
3. etl.py: all of our job is in this file where we open a spark session, extract data from s3, transform data and load it to our own s3 path


## Data Schema <a name="schema"></a>

following is a diagram that dipicts our dimentional model that represents a star schema:
![](SCHEMA.png?raw=true)
            

## Instructions to run the files<a name="inst"></a>

1. Enter your aws account info in dwh.cfg
2. Enter your directory path, where you need your dimentional model to be written to in etl.py main() section
3. open terminal and navigate to project folder and run etl.py file or run it in your emr terminal

## Acknowledgements<a name="licensing"></a>
Credits go to udacity for providing the opportunity to practice our skills!
