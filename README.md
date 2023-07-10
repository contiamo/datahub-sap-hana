# Datahub SAP Hana Metadata source

Add your SAP Hana databases to your Linkedin Datahub!

# Description
This python package extracts views metadata from SAP Hana db to create table and column lineage to Datahub. 
The script for the source file supports ingestion of both table and column lineage via the Datahub CLI in one yaml recipe file. 
The ingestion config file can also specify specific schemas to include/ exclude and allows for the creation of lineage across different schemas in a SAP Hana db. 
Results can be seen in the Datahub UI or printed in the console, or file. 


## Installing

Pre-built Wheels can be downloaded from the [Releases page](https://github.com/contiamo/datahub-sap-hana/releases/latest)

Otherwise, you must install from source.

### Requirements
You need the following tools pre-installed
* [Task](https://taskfile.dev/#/installation)
* [Poetry](https://python-poetry.org/docs/#installation)
* [Pyenv](https://github.com/pyenv/pyenv#installation)
  * Make sure to have the [required build packages installed for your OS](https://github.com/pyenv/pyenv/wiki#suggested-build-environment)
  * And the [shims for your shell](https://github.com/pyenv/pyenv#set-up-your-shell-environment-for-pyenv)


## Try it out

1. Clone the project

   ```sh
   git clone git@github.com:contiamo/datahub-sap-hana.git
   cd datahub-sap-hana
   ```

2. You will need Python 3.10 or higher

   Once you have `pyenv` and Poetry installed, you should run

   ```sh
   pyenv install 3.10.10
   pyenv local 3.10.10
   poetry config virtualenvs.in-project true
   ```


3. Install the project and dependencies

   ```sh
   task setup
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
   ingest run -c /opt/examples/hana_recipe.yaml
```
Note that you may need to set the `--network` flag if you are using the Hana Express Docker image.

## Development

### Running the tests

To run the unit tests, use

```sh
task test
```

To run all of the tests, just use

```sh
task test -- -v
```
