#!/bin/bash
echo $CRED_PROD > /root/.basedosdados/credentials/prod.json
echo $CRED_STAG > /root/.basedosdados/credentials/staging.json
celery -A tasks beat -l debug
