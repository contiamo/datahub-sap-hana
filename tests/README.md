### Notes on Testing 

### Getting Started
- Get SAP HANA express.
1. Make sure that you have a dockerhub account. Otherwise, create one [here](https://hub.docker.com/).
2. Pull the [hanaexpress](https://hub.docker.com/r/saplabs/hanaexpress) docker image from the dockerhub library. 
3. Copy the docker-compose [file](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/integration/hana/docker-compose.yml) from the datahub integration tests. Also copy the post_start and setup folders from datahub. 
4. Modify the docker-compose file to use the new image name: 
```image: "saplabs/hanaexpress:2.00.061.00.20220519.1"```

- Test the SAP HANA connection
5. To start the SAP HANA db: 
```docker-compose up -d```

6. Confirm db connection with sqlalchemy. Run ```poetry run ipython.```
In ipython: 
```
from sqlalchemy import create_engine
engine = create_engine("hana://username:password@localhost:host_port/database_name")
conn = engine.connect()
results = conn.execute('''SELECT USER_NAME FROM SYS.USERS''')
```
The results should show that the database HOTEL is in the system: 

```[('SYS',), ('SYSTEM',), ('_SYS_STATISTICS',), ('_SYS_TABLE_REPLICAS',), ('_SYS_EPM',), ('_SYS_REPO',), ('_SYS_SQL_ANALYZER',), ('_SYS_TASK',), ('_SYS_AFL',), ('_SYS_WORKLOAD_REPLAY',), ('_SYS_DATA_ANONYMIZATION',), ('_SYS_ADVISOR',), ('_SYS_PLAN_STABILITY',), ('HOTEL',)]```



### Testing 
The test checks for the db connection by running a sample query based on the HOTEL db. 
It also checks a golden file containing the metadata of workunits against the expected metadata output of an ingestion. 

1. In the terminal of the project, run:
```task test -- --update-golden-files```

