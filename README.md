# Datahub SAP Hana Metadata source

Add your SAP Hana databases to your Linkedin Datahub!

## Try it out

1. You will need Python 3.9, [Poetry](https://python-poetry.org/docs/#installation), and [`pyenv`](https://github.com/pyenv/pyenv).

   Once you have `pyenv` and Poetry installed, you should run

   ```sh
   pyenv install 3.9.7
   pyenv local 3.9.7
   poetry config virtualenvs.in-project true
   ```

2. Install the project and dependencies

   ```sh
   poetry install
   ```

3. Start the test foodmart db, this requires [Docker and Docker Compose](https://docs.docker.com/get-docker/)

   ```sh
   docker-compose up -d
   ```

   This is running the foodmart db from https://github.com/contiamo/foodmart-data

4. Run the test sync

   ```sh
   poetry run datahub ingest run -c hana_recipe.yaml
   ```
