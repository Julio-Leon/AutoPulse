#!/bin/bash
# =====================================================================
# AutoPulse | Pull the latest code bundle and restart changed services.
# Run on the EC2 host (infra/aws/ec2_host.py update does this through SSM):
#     sudo /opt/autopulse/app/deploy/ec2/update.sh
# Data (Kafka, simulator state, Airflow DB) lives in Docker volumes and is kept.
# =====================================================================
set -euxo pipefail
exec > >(tee -a /var/log/autopulse-update.log) 2>&1

APP_DIR=/opt/autopulse
source "$APP_DIR/autopulse.env"

aws s3 cp "s3://${BUCKET}/artifacts/ec2/autopulse-ec2-bundle.tar.gz" /tmp/autopulse-ec2-bundle.tar.gz --region "$AWS_REGION"
rm -rf "$APP_DIR/app.new"
mkdir -p "$APP_DIR/app.new"
tar -xzf /tmp/autopulse-ec2-bundle.tar.gz -C "$APP_DIR/app.new"
cp "$APP_DIR/autopulse.env" "$APP_DIR/app.new/deploy/ec2/.env"

rm -rf "$APP_DIR/app.old"
[ -d "$APP_DIR/app" ] && mv "$APP_DIR/app" "$APP_DIR/app.old"
mv "$APP_DIR/app.new" "$APP_DIR/app"

cd "$APP_DIR/app/deploy/ec2"
docker compose up -d --build --remove-orphans
# The DAG folder is a bind mount of the (replaced) code directory, so restart
# the Airflow services to pick up the new DAG files.
docker compose up -d --force-recreate --no-deps airflow-scheduler airflow-webserver
docker compose ps

echo "AutoPulse update finished at $(date -Is)"
