#!/bin/bash
set -euo pipefail

app="benchmarks-app"
app_dir="app"
tag="${TAG:-latest}"
tagged_image="${app}:${tag}"

echo "Creating package in dist directory for $tagged_image image..."
echo "Preparing dist dir..."

rm -r -f dist
mkdir dist

echo "Building jar..."

mvn clean install

echo "Building image..."

docker build . -t "$tagged_image"

gzipped_image_path="dist/$app.tar.gz"

echo "Image built, exporting it to $gzipped_image_path, this can take a while..."

docker save "$tagged_image" | gzip > ${gzipped_image_path}

echo "Image exported, preparing scripts..."

export DB0_HOST=${DB0_HOST:-localhost}
export DB0_URL="jdbc:postgresql://$DB0_HOST:5432/events"
export DB0_ENABLED=${DB0_ENABLED:-true}

export DB1_HOST=${DB1_HOST:-localhost}
export DB1_URL="jdbc:postgresql://$DB1_HOST:5432/events"
export DB1_ENABLED=${DB1_ENABLED:-true}

export DB2_HOST=${DB2_HOST:-localhost}
export DB2_URL="jdbc:postgresql://$DB2_HOST:5432/events"
export DB2_ENABLED=${DB2_ENABLED:-true}

export app=$app
export tag=$tag
export run_cmd="docker run -d \\
  -e DB0_URL=\"$DB0_URL\" -e DB0_ENABLED=\"$DB0_ENABLED\" \\
  -e DB1_URL=\"$DB1_URL\" -e DB1_ENABLED=\"$DB1_ENABLED\" \\
  -e DB2_URL=\"$DB2_URL\" -e DB2_ENABLED=\"$DB2_ENABLED\" \\
  --network host \\
  --name $app $tagged_image"

cd ..
envsubst '${app} ${tag}' < scripts/template_load_and_run_app.bash > $app_dir/dist/load_and_run_app.bash
envsubst '${app} ${run_cmd}' < scripts/template_run_app.bash > $app_dir/dist/run_app.bash

echo "Package prepared."