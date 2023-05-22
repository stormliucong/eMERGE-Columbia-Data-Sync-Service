#!/bin/bash

/home/cl3720/miniconda3/bin/python /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/data_pull_from_r4.py \
--log /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/data_pull_from_r4.log \
--token /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/api_tokens.json \
--ignore /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/ignore_R4_fields.json \
2> /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/data_pull_from_r4_errors.log


/home/cl3720/miniconda3/bin/python /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/epic_id_conversion.py \
--log /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/empi_convert.log \
--token /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/api_tokens.json \
2> /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/empi_convert_errors.log

/home/cl3720/miniconda3/bin/python /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/get_provider_npi.py \
--log /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/get_provider_npi.log \
--token /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/api_tokens.json \
2> /phi_home/cl3720/phi/eMERGE/eIV-recruitement-support-redcap/get_provider_npi_errors.log