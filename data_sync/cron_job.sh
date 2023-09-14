#!/bin/bash

/home/ubuntu/eMERGE-Columbia-Data-Sync-Service/.venv/bin/python \
/home/ubuntu/eMERGE-Columbia-Data-Sync-Service/data_sync/data_pull_from_r4.py \
--log /home/ubuntu/eMERGE-Columbia-Data-Sync-Service/data_sync/logs/ \
--token /home/ubuntu/eMERGE-Columbia-Data-Sync-Service/api_tokens.json \
--ignore /home/ubuntu/eMERGE-Columbia-Data-Sync-Service/data_sync/ignore_R4_fields.json