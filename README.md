# Sparkify migration to AWS Redshift

## 1) Goal
### The purpose of this project is to relocate sparkify's on-prem DWH to AWS Redshift for all it's benefits including scalability, robustness, and security, while maintaing all functionalities of a DWH


## 2) Design
### The work plan was to first load the data from both sources in S3 into staging tables, to be handy in aggregating or manipulating the data before inserting it into the star schema tables, which consists of one fact table (song_plays), and many dimension tables each focusing on a specific entity ( users, songs, time, etc.)


## 3) Files used in the project
###   a) sql_queries.py : This file contains all queries, drop tables, create tables, copy  staging tables, and insert star schema tables 
###   b) dwh.cfg : This file contains all the parameters used for our Redshift cluster and data stored in the S3 bucket, we load it in our scripts using [ConfigParser](https://docs.python.org/3/library/configparser.html) 
###   c) create_tables.py : This script takes the drop tables and the create tables queries and executes them, it should be run from the terminal 
###   d) etl.py : This script takes the copy and insert tables queries and executes them, it should be run from the terminal


## 4) Conclusion
### migrating to AWS is expected to be a good move for sparkify as it promises a lot of scalability and security

      
