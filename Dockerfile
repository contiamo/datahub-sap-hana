FROM linkedin/datahub-ingestion:${DATAHUB:-v0.8.22}

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt


COPY dist/ ./dist/
RUN pip install --no-index --find-links=dist/ datahub_sap_hana
