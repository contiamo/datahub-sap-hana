### Notes on Testing 

### Getting Started
- Get SAP HANA express.
1. Make sure that you have a dockerhub account. Otherwise, create one [here](https://hub.docker.com/).
2. Pull the [hanaexpress](https://hub.docker.com/r/saplabs/hanaexpress) docker image from the dockerhub library. 
3. Copy the docker-compose [file](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/integration/hana/docker-compose.yml) from the datahub integration tests. 
4. Modify the docker-compose file to use the new image name: 
```image: "saplabs/hanaexpress:2.00.061.00.20220519.1"```

- Test the SAP HANA connection
5. To start the SAP HANA db: 
```docker-compose up -d```

6. Confirm db connection with sqlalchemy:
```
from sqlalchemy import create_engine
engine = create_engine("hana://username:password@localhost:host_port/database_name")
conn = engine.connect()
results = conn.execute("SELECT STATEMENT HERE")
```

### Testing 
1. In the terminal of the project, run:
```pytest integration_tests/test_hana.py`

