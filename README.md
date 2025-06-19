Project: Data Warehouse


1.Created cluster in redshift and connected to its default database 'dev' to store the tables which are retrieved from the source s3 bucket.

2.Queries are written in sql_queries.py to create, insert, drop the staging tables and final tables.

3. The Data should be copyied to the staging tables from the s3 bucket.

4. Then the Final tables values are retreived from the staging tables.

5. Queries can be performed on the redshift query editors.
