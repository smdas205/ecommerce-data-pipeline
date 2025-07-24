A project to implement data engineering concepts

Purpose: To ingest, process and analyze data logs from e-commerce websites. 

Softwares used:
Programming Languages: Python, SQL
Databases: MySQL
Frameworks: Hadoop, Spark
API: PySpark
Version Control: Git


Metadata Schema
field             data_type         description
user_id           string            Stores the User ID of the customer
type_of_event     string            Mentions the type of event based on what happened. eg: view, cart, purchase, return
timestamp         datetime          Timestamp at which the event occured
product_id        string            Unique ID of the product for which the specified event occured for the customer
price             float             Price of the product.

For price:
If purchase event, value will be positive, reflecting profit to company
If return event, value will be negative, reflecting loss to company
If event view or cart, value will be zero, indicating no revenue change with these actions to the company