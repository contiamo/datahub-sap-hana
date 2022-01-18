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
   poetry install
   ```

4. Start the test foodmart db, this requires [Docker and Docker Compose](https://docs.docker.com/get-docker/)

   ```sh
   docker-compose up -d
   ```

   This is running the foodmart db from https://github.com/contiamo/foodmart-data

5. Run the test sync

   ```sh
   poetry run datahub ingest run -c hana_recipe.yaml
   ```

6. Inspect the contents of the `hana_mces.json` file that was created.

## Development

### Running the tests

To run the unit tests, use

```sh
poetry run pytest -v -m 'not integration'
```

To run all of the tests, just use

```sh
poetry run pytest -v
```

Note that the integration test suite is still WIP.
