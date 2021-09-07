# udacity-airflow-with-AWSS3-Guided-Project
Use airflow to establish ETL scheduling services (DAG). Execute SQL for creates table in user AWS S3 bucket and insert data from another S3 bucket for staging and relational modeling accord to time schedule and sensor. 


## Project Objective:
- Design ETL process with Airflow for psudo company `Sparkify`
- Create Redshift cluster, Creates tables within cluster 
- Insert data from `udacity-dend` S3 bucket into Personal S3 bucket
- Two data sources --> .csv log_data sources (s3://udacity-dend/log_data)  and JSON metadata (s3://udacity-dend/song_data) record of songs that user consumed
- Customize Airflow Operator and dag base project requriement
- Create Data quality check, ensuring there's data after inserted

## Project Dependencies requirement
- airflow
- Redshift connection with AWS
- AWS credential

## DAG
udac_example_dag.py --> Declare working dag and task dependencies 

## Airflow Custom Dag Operator description
- Stage_redshift.py --> Customized redshift and AWS credential input, initiate copy data from `udacity-dend` S3 bucket into Redhsift cluster
- Load_dimension.py / Load_fact.py --> customize Airflow operator to load dimension and fact table by executing SQL from `sql_queries.sql` file
- data_quality.py --> Take in SQL query in execute to check if inserted data sucessful by counting number of records if more than 0

## Support file of SQL, config, and table creation
- Create_tables.sql --> SQL for creates tables if not existed within redshift
- Create_table_script.py --> console execute `create_table.sql` with redshift 
- dwh.cfg --> AWS crendential access point  
