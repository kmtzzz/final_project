# Final project

### Description
This repository is intended for source code of diploma project.  

Technologies used in implementation: (italic)
1. S3
2. Postgres
3. Vertica
4. Python
5. Airflow
6. Docker
7. Metabase

### Repository structure
Inside `src` next folders exist:
- `/src/dags` - DAG, which extracts data from S3 and loads to Vertica STAGING data layer, DAG name is `1_data_import.py`. DAG updating data mart, DAG name is `2_datamart_update.py`.
- `/src/sql` - DDLs for database model definition in `STAGING`- and `DWH`- layers, as well as script for datamart upload.
- `/src/py` - in case of Kafka is source, place code for producing and consuming messages from it.
- `/src/img` -  screenshot of Metabase dashboard built on top of datamart.
