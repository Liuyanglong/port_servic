mkdir -p output
rm -rf output/*
cp apollo_service output/
cp service_config.cfg output/
cp -r bin/ output/
cp  bin/control output/run.sh
