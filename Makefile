DATAPROC_BUCKET = gs://dataproc-work-dir
CUR_DIR := $(shell pwd)
GOOGLE_APPLICATION_CREDENTIALS=$(CUR_DIR)/gcp-credentials.json

sync-scripts: #Syncs local scripts with GCS scripts directory 
	cd ./scripts && find . -maxdepth 1 -mindepth 1 -type d -exec sh -c 'zip -r "{}.zip" "{}"' \;
	GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS} gsutil cp ./scripts/*.zip $(DATAPROC_BUCKET)/pyfiles/
	rm -rf ./scripts/*.zip
	GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS} gsutil -m rsync -d -r ./scripts/ $(DATAPROC_BUCKET)/scripts/