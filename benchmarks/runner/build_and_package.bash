#!/bin/bash
set -euo pipefail

app="benchmarks-runner"
app_dir="runner"
tag="${TAG:-latest}"
tagged_image="${app}:${tag}"

echo "Creating package in dist directory for $tagged_image image..."
echo "Preparing dist dir..."

echo "Building jar..."
mvn clean install

rm -r -f dist
mkdir dist

echo "Building image..."

docker build . -t "${tagged_image}"

gzipped_image_path="dist/$app.tar.gz"

echo "Image built, exporting it to $gzipped_image_path, this can take a while..."

docker save "${tagged_image}" | gzip > ${gzipped_image_path}

echo "Image exported, preparing scripts..."

export app=$app
export tag=$tag
export run_cmd="docker run -d \\
  -e EVENTS_RATE -e EVENTS_TO_PUBLISH -e RUNNER_INSTANCES \\
  --memory 2G --cpus 4 \\
  --network host \\
  --name $app $tagged_image"

cd ..
envsubst '${app} ${tag}' < scripts/template_load_and_run_app.bash > $app_dir/dist/load_and_run_app.bash
envsubst '${app} ${run_cmd}' < scripts/template_run_app.bash > $app_dir/dist/run_app.bash

echo "Package prepared."