FROM acryldata/datahub-ingestion:${DATAHUB:-v0.10.3.1}

COPY dist/ ./dist/
RUN pip install --no-index --find-links=dist/ datahub_sap_hana
