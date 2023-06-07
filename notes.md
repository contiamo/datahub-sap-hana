
## Notes on SAP Hana file lineage ingestion  

### Connecting to SAP Hana

1. Run the datahub-sap-hana-example database using ```docker-compose up -d```. Follow instructions [here].(https://github.com/contiamo/datahub-sap-hana-example)

2. Follow the instructions in the [repo](https://github.com/contiamo/datahub-sap-hana) to install basic datahub_sap_hana metadata ingestion.

3. In the folder where the repo datahub-sap-hana, use ipython to query the database. In the command line:
```

4. To create a connection to the db using python:

[Connect to SAP HANA from Python](https://help.sap.com/docs/SAP_HANA_PLATFORM/0eec0d68141541d1b07893a39944924e/d12c86af7cb442d1b9f8520e2aba7758.html)

  

``` from hdbcli import dbapi

conn = dbapi.connect(address='localhost', port=39044, user='pantheon', password='YDmB6vVbgUyfDT')

```  

### Querying data

In ipython, the general format to execute a query and see the results is:

```
cursor = conn.cursor()

cursor.execute("SELECT SQL STATEMENT")

# returns True

for row in cursor:

print(row)

```

1. To get all schemas:

``` cursor. execute("SELECT * from SCHEMAS") ```

2. To get all tables:

```cursor. execute("SELECT * from TABLES") ```

For a specific table:

```cursor. execute("SELECT * from TABLES WHERE TABLE_NAME='product'")```

For tables in a specific schema:

```cursor. execute("SELECT * from TABLES WHERE SCHEMA_NAME='PANTHEON'")```

3. To get all views:

```cursor. execute("SELECT * from VIEWS")```

For views in a specific schema

```cursor. execute("SELECT * from SYS.VIEWS WHERE SCHEMA_NAME='PANTHEON'")```

4. Query a table and show 10 rows

``` cursor.execute('''SELECT * from "PANTHEON"."product" limit 10''')```

5. SQL Definition

```cursor.execute("SELECT DEFINITION FROM SYS.VIEWS WHERE VIEW_NAME='product_sales_1997'")```

6. To get dependent objects


```cursor. execute("SELECT * from SYS.OBJECT_DEPENDENCIES")```

To get the table/s that feed into view 'product_sales_1997':

```cursor. execute("SELECT * from SYS.OBJECT_DEPENDENCIES WHERE DEPENDENT_OBJECT_NAME='product_sales_1997'")```

To see the table/s dependent on another table such as 'sales_fact_1997':

```cursor. execute("SELECT * from SYS.OBJECT_DEPENDENCIES WHERE BASE_SCHEMA_NAME='PANTHEON' AND BASE_OBJECT_NAME='sales_fact_1997'")```

[Documentation](https://help.sap.com/docs/HANA_SERVICE_CF/7c78579ce9b14a669c1f3295b0d8ca16/20cbd12e7519101489c7cfcd0f32868d.html) for object dependencies.

### Connecting with sqlalchemy 

- The credentials for the db should be used to create a sql_url for the engine object to establish connection with the SAP HANA db. 

```
from sqlalchemy import create_engine
engine = create_engine("hana://username:password@localhost:host_port/database_name")
conn = engine.connect()
results = conn.execute("SELECT STATEMENT HERE")
```
### Table lineage structure

```yaml
version: 1
lineage:
  - entity:
      name: product_sales_1997 #in datahub, dependent_object_name
      type: dataset
      env: DEV
      platform: hana # not sure if this is the right name
    upstream:
      - entity:
          name: sales_fact_1997 #in datahub, base_object_name
          type: dataset
          env: DEV
          platform: hana
      - entity:
          name: product #in datahub, base_object_name
          type: dataset
          env: DEV
          platform: hana
```

# Other info: 
- The script not only captures the object dependency between tables and views, it also considers the foreign-key dependencies between tables