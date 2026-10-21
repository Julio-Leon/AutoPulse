#!/bin/bash
# =====================================================================
# AutoPulse | EC2 first-boot setup (EC2 user data, runs once as root).
# infra/aws/ec2_host.py fills in __BUCKET__ and __REGION__ and passes this
# script as the instance's user data. Output: /var/log/autopulse-bootstrap.log
# =====================================================================
set -euxo pipefail
exec > >(tee -a /var/log/autopulse-bootstrap.log) 2>&1

BUCKET="__BUCKET__"
REGION="__REGION__"
APP_DIR=/opt/autopulse
COMPOSE_VERSION=v2.29.7
BUILDX_VERSION=v0.17.1

# 1. Docker + Compose + Buildx
dnf install -y docker
systemctl enable --now docker
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
curl -fsSL "https://github.com/docker/buildx/releases/download/${BUILDX_VERSION}/buildx-${BUILDX_VERSION}.linux-amd64" \
  -o /usr/local/lib/docker/cli-plugins/docker-buildx
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/lib/docker/cli-plugins/docker-buildx

# 2. 2 GB swap as a safety margin for Spark + Airflow on 8 GB RAM
if [ ! -f /swapfile ]; then
  dd if=/dev/zero of=/swapfile bs=1M count=2048
  chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  echo '/swapfile swap swap defaults 0 0' >> /etc/fstab
fi

# 3. Settings (kept across updates)
mkdir -p "$APP_DIR"
if [ ! -f "$APP_DIR/autopulse.env" ]; then
  cat > "$APP_DIR/autopulse.env" <<ENV
BUCKET=${BUCKET}
AWS_REGION=${REGION}
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-20)
ENV
  chmod 600 "$APP_DIR/autopulse.env"
fi

# 4. Code: latest bundle uploaded by infra/aws/deploy.py
aws s3 cp "s3://${BUCKET}/artifacts/ec2/autopulse-ec2-bundle.tar.gz" /tmp/autopulse-ec2-bundle.tar.gz --region "$REGION"
mkdir -p "$APP_DIR/app"
tar -xzf /tmp/autopulse-ec2-bundle.tar.gz -C "$APP_DIR/app"
cp "$APP_DIR/autopulse.env" "$APP_DIR/app/deploy/ec2/.env"

# 5. Start everything (restart policies bring it back after a reboot)
cd "$APP_DIR/app/deploy/ec2"
docker compose up -d --build

echo "AutoPulse bootstrap finished at $(date -Is)"
