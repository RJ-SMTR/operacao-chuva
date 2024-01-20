#!/bin/bash
echo $CRED_PROD > /root/.basedosdados/credentials/prod.json
echo $CRED_STAG > /root/.basedosdados/credentials/staging.json
streamlit run src/app.py --server.port=80 --server.address=0.0.0.0