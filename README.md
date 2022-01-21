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

4. Edit the `examples/hana_recipe.yaml` to set the connection details to your SAP Hana database.

   If you just want to do a local test, SAP offers [SAP Hana Express](https://www.sap.com/products/hana/express-trial.html) as a
   free trial version of Hana. There is also [a Docker image](https://developers.sap.com/tutorials/hxe-ua-install-using-docker.html)
   that makes this very easy, this is our recommendation.

5. Run the test sync

   ```sh
   poetry run datahub ingest run -c examples/hana_recipe.yaml
   ```

6. Inspect the contents of the `hana_mces.json` file that was created.

### Docker image

A Docker image with datahub and this package preinstalled is provided via the [Github Container Registry, see here](https://github.com/contiamo/datahub-sap-hana/pkgs/container/datahub-sap-hana)

```sh
docker run -it --rm -v `pwd`:/opt \
   ghcr.io/contiamo/datahub-sap-hana:latest \
   ingest run -c /opt/examples/ha.yaml
```

Note that you may need to set the `--network` flag if you are using the Hana Express Docker image.

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
