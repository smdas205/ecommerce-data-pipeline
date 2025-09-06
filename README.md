A project to implement data engineering concepts  

Purpose: To ingest, process and analyze data logs from e-commerce websites.   

Softwares used:  
Programming Languages: Python, SQL  
Databases: MySQL  
Frameworks: Hadoop, Spark  
API: PySpark  
Version Control: Git  


Metadata Schema  


|field         |data_type |description                                                                         |  
|:-------------|:--------:|-----------------------------------------------------------------------------------:|  
|user_id       |string    |Stores the User ID of the customer                                                  |  
|type_of_event |string    |Mentions the type of event based on what happened. eg: view, cart, purchase, return |  
|timestamp     |datetime  |Timestamp at which the event occured                                                |  
|product_id    |string    |Unique ID of the product for which the specified event occured for the customer     |  
|price         |float     |Price of the product.                                                               |  


For price:  
If purchase event, value will be positive, reflecting profit to company  
If return event, value will be negative, reflecting loss to company  
If event view or cart, value will be zero, indicating no revenue change with these actions to the company  

Process:  

Phase 1:  
Created the initial project structure and created Git and GitHub repository.  

Phase 2:  
Generated random logs onto a comma-separated file (data-generator/random.csv) using a python script (data-generator/log-generator.py).  

Phase 3:  
Using a PySpark script (pyspark-jobs/process_logs.py), converted raw logs into analytical tables, which were saved as .csv files.  

Phase 4:  
Transferred the anaytical .csv tables to Hive, created Hive database 'ecommerce' where previous .csv files were turned into external tables. Also performed some queries to check for proper Hadoop - Hive integration.  