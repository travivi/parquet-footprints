node -r ts-node/register ./tsOutputs/index.js generate-query \
    --customer-id=staging \
    --app-name=sealights-sl-cloud-release \
    --branch-name=dev \
    --build-name=backend-infra-20240605-114703 \
    --bsid=42d15e2c-26fa-48cd-a766-37873a480e25 \
    --source-db-host=euw-dev-staging-db-main-a.dev.sealights.co \
    --source-db-name=SeaLightsDB
