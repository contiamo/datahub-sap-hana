#!/bin/bash
# This is copied and slightly modified from the acryl-datahub project
# Copyright 2015 LinkedIn Corp. All rights reserved.
# The original source code can be found at https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/integration/hana/post_start/import_sql.sh
# And the original Apache 2 license is available at https://github.com/acryldata/datahub/blob/master/LICENSE

# This is copied and slightly modified from the acryl-datahub project
# Copyright 2015 LinkedIn Corp. All rights reserved.
# The original source code can be found at https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/integration/hana/post_start/import_sql.sh
# And the original Apache 2 license is available at https://github.com/acryldata/datahub/blob/master/LICENSE



set -euo pipefail

#found in /run_hana.sh, hxe_optimize.sh
#durinng the 'initial' phase there is key for SYSTEM available
declare -r tenant_store_key=us_key_tenantdb

# import dump


function main() {
    case "$_HOOK_START_TYPE" in
        initial)
            echo "Running initial import"
            # create user
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 "CREATE USER $SCHEMA_NAME PASSWORD \"$SCHEMA_PWD\" NO FORCE_FIRST_PASSWORD_CHANGE" 2>&1
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 "ALTER USER $SCHEMA_NAME DISABLE PASSWORD LIFETIME" 2>&1
            # import dump
            /usr/sap/HXE/HDB90/exe/hdbsql -a -x -i 90 -d HXE -U ${tenant_store_key} -B UTF8 -I "/hana/mounts/setup/$DUMP_FILE" 2>&1
        ;;
        *)
            echo "Unknown hook type $_HOOK_START_TYPE"
            ;;
    esac
}
main