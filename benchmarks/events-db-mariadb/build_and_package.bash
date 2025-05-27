#!/bin/bash
set -euo pipefail

app="events-db-mariadb"

tag="${TAG:-latest}"
tagged_image="${app}:${tag}"
volume_dir="${EVENTS_DB_VOLUME_DIR:-/home/eventsql/${app}_volume}"
volume="-v $volume_dir:/var/lib/mysql"
memory_limit="${MEMORY_LIMIT:-8G}"
cpus_limit="${CPUS_LIMIT:-4}"

echo "Creating package in dist directory for $tagged_image image..."
echo "Preparing dist dir..."

rm -r -f dist
mkdir dist

echo "Building image..."

docker build . -t "$tagged_image"

gzipped_image_path="dist/$app.tar.gz"

echo "Image built, exporting it to $gzipped_image_path, this can take a while..."

docker save "$tagged_image" | gzip > "$gzipped_image_path"

echo "Image exported, preparing scripts..."

export app=$app
export tag=$tag
export run_cmd="docker run -d \\
  -e \"MARIADB_ROOT_PASSWORD=mariadb\"  \\
   --memory ${memory_limit} --cpus ${cpus_limit} \
  --network host ${volume} --name $app $tagged_image"

cd ..
envsubst '${app} ${tag}' < scripts/template_load_and_run_app.bash > "$app/dist/load_and_run_app.bash"
envsubst '${app} ${run_cmd}' < scripts/template_run_app.bash > "$app/dist/run_app.bash"

echo "Package prepared."