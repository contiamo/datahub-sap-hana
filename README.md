# Datahub SAP Hana Metadata source

Add your SAP Hana databases to your Linkedin Datahub!

## Installing

Pre-built Wheels can be downloaded from the [Releases page](https://github.com/contiamo/datahub-sap-hana/releases/latest)

Otherwise, you must install from source.

## Try it out

1. You will need Python 3.9, [Poetry](https://python-poetry.org/docs/#installation), and [`pyenv`](https://github.com/pyenv/pyenv).

   Once you have `pyenv` and Poetry installed, you should run

   ```sh
   pyenv install 3.9.7
   pyenv local 3.9.7
   poetry config virtualenvs.in-project true
   ```

2. Clone the project

   ```sh
   git clone git@github.com:contiamo/datahub-sap-hana.git
   cd datahub-sap-hana
   ```

3. Install the project and dependencies

   ```sh
   make setup
   ```

4. Start the test foodmart db, this requires [Docker and Docker Compose](https://docs.docker.com/get-docker/)

   ```sh
   docker-compose up -d
   ```

   This is running the foodmart db from https://github.com/contiamo/foodmart-data

   This requires access to our DEV Docker registry. You need to be a Contiamo developer to use this.

   Install the [`glcoud` CLI](https://cloud.google.com/sdk/docs/install) and then run these commands

   ```sh
   gcloud auth login
   gcloud auth configure-docker
   ```

5. Run the test sync

   ```sh
   poetry run datahub ingest run -c examples/hana_recipe.yaml
   ```

6. Inspect the contents of the `hana_mces.json` file that was created.

_Alternative_:
You can use docker to run the ingestion like this

```sh
docker run -it --rm -v `pwd`:/opt \
   --network datahub-sap-hana_default \
   contiamo/datahub-sap-hana:latest \
   ingest run -c /opt/examples/hana_compose.yaml
```

## Development

### Running the tests

To run the unit tests, use

```sh
make test
```

To run all of the tests, just use

```sh
poetry run pytest -v
```

Note that the integration test suite is still WIP.
