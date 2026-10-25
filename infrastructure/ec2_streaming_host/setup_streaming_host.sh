#!/usr/bin/env bash
# AutoPulse - set up (or update) the EC2 streaming host.
#
# This host runs the near-real-time half of the pipeline, 24/7:
#
#   vehicle simulator  ->  Kafka (Docker)  ->  Spark stream  ->  s3://autopulse-julio-leon/bronze/vehicle_telemetry/
#
# The simulator and the stream run as systemd services, so they start when the
# instance boots and restart on their own if they crash.
#
# Sized for a t3.micro (1 GB memory): Kafka gets 256 MB, Spark 512 MB, and the
# script adds a 2 GB swap file as a safety buffer so nothing is killed when
# memory runs short.
#
# The code comes from S3 (uploaded from your laptop to deployment/autopulse/).
# Run on the instance (EC2 -> Connect -> Session Manager):
#
#   aws s3 cp s3://autopulse-julio-leon/deployment/autopulse/infrastructure/ec2_streaming_host/setup_streaming_host.sh /tmp/
#   sudo bash /tmp/setup_streaming_host.sh
#
# Safe to run again: after uploading changed code to S3, run it again to copy
# the new code and restart the services.

set -euo pipefail

BUCKET="autopulse-julio-leon"
CODE_S3="s3://${BUCKET}/deployment/autopulse"
APP_USER="ec2-user"
APP_DIR="/home/${APP_USER}/AutoPulse"
VENV_DIR="/home/${APP_USER}/autopulse-venv"
COMPOSE_VERSION="v2.29.7"

echo "== 0/5  Swap file (2 GB of disk used as extra memory when RAM runs out)"
if [ ! -f /swapfile ]; then
    dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none
    chmod 600 /swapfile
    mkswap /swapfile
    echo "/swapfile none swap sw 0 0" >> /etc/fstab      # keep it after reboots
fi
swapon /swapfile 2>/dev/null || true                     # already on: nothing to do

echo "== 1/5  System packages: Docker, Java 17 (needed by Spark), Python 3.11"
dnf install -y docker java-17-amazon-corretto-headless python3.11 python3.11-pip

echo "== 2/5  Docker, plus the 'docker compose' plugin (not packaged for Amazon Linux)"
systemctl enable --now docker
usermod -aG docker "${APP_USER}"          # lets the simulator service start Kafka
if ! docker compose version >/dev/null 2>&1; then
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-$(uname -m)" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

echo "== 3/5  Project code from ${CODE_S3}/"
mkdir -p "${APP_DIR}"
# --delete removes files that were deleted from S3; data/ (the simulator's fleet state) is kept.
aws s3 sync "${CODE_S3}/" "${APP_DIR}/" --delete --exclude "data/*"
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

echo "== 4/5  Python environment (kafka-python, pyspark)"
# pip downloads and unpacks in a temporary folder. /tmp lives in memory and is only
# about half the RAM (~450 MB on a t3.micro), smaller than PySpark (~450 MB download),
# so use a folder on disk instead.
PIP_TEMP_DIR="/var/tmp/autopulse-pip"
mkdir -p "${PIP_TEMP_DIR}" && chown "${APP_USER}:${APP_USER}" "${PIP_TEMP_DIR}"
if [ ! -d "${VENV_DIR}" ]; then
    sudo -u "${APP_USER}" python3.11 -m venv "${VENV_DIR}"
fi
sudo -u "${APP_USER}" env TMPDIR="${PIP_TEMP_DIR}" "${VENV_DIR}/bin/pip" install --quiet --upgrade pip
sudo -u "${APP_USER}" env TMPDIR="${PIP_TEMP_DIR}" "${VENV_DIR}/bin/pip" install --quiet --no-cache-dir \
    -r "${APP_DIR}/requirements.txt"
rm -rf "${PIP_TEMP_DIR}"

echo "== 5/5  Services: install, enable at boot, (re)start"
cp "${APP_DIR}"/infrastructure/ec2_streaming_host/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable autopulse-vehicle-simulator.service autopulse-telemetry-stream.service
systemctl restart autopulse-vehicle-simulator.service autopulse-telemetry-stream.service

echo
echo "Done. Check the services with:"
echo "  systemctl status autopulse-vehicle-simulator autopulse-telemetry-stream"
echo "  journalctl -u autopulse-vehicle-simulator -f      (Ctrl+C to stop watching)"
echo "  journalctl -u autopulse-telemetry-stream -f"
