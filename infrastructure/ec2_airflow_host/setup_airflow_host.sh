#!/usr/bin/env bash
# AutoPulse - set up (or update) the Airflow host.
#
# Apache Airflow orchestrates and monitors the whole pipeline (DAGs in
# orchestration/airflow_dags/):
#   - runs the Glue jobs in order (collisions daily, telemetry every 15 minutes)
#     and validates the data lake after each run;
#   - checks the streaming host and Snowflake (Snowpipe, dynamic tables, data quality).
#
# Airflow 3. What runs on this host:
#   PostgreSQL             Airflow's own database (run history, settings)
#   airflow-dag-processor  reads the DAG files and saves them in the database
#   airflow-scheduler      starts DAG runs on time and runs their tasks
#   airflow-api-server     the Airflow web page (port 8080) and the API that
#                          running tasks report back to
#
# Airflow 3 recommends 4 GB of memory: use a t3.medium. On a t3.small (2 GB) it
# runs, leaning on the 4 GB swap file; lower AIRFLOW__CORE__PARALLELISM to 2 there.
#
# Upgrading this host from Airflow 2: the script notices the old version, removes
# its Python environment and the old webserver service, and upgrades Airflow's
# database in place (run history is kept).
#
# Run on the instance (EC2 -> Connect -> Session Manager):
#
#   aws s3 cp s3://autopulse-julio-leon/deployment/autopulse/infrastructure/ec2_airflow_host/setup_airflow_host.sh /tmp/
#   sudo bash /tmp/setup_airflow_host.sh
#
# Safe to run again: after uploading changed code (e.g. DAGs) to S3, run it again to
# copy the new code and restart Airflow. Passwords and the Snowflake key are kept.
#
# At the end it prints the Airflow login and the Snowflake public key
# (paste the key into snowflake/10_airflow_monitoring_user.sql).

set -euo pipefail

BUCKET="autopulse-julio-leon"
CODE_S3="s3://${BUCKET}/deployment/autopulse"
APP_USER="ec2-user"
APP_DIR="/home/${APP_USER}/AutoPulse"
AIRFLOW_HOME="/home/${APP_USER}/airflow"
VENV_DIR="/home/${APP_USER}/airflow-venv"
AIRFLOW_VERSION="3.3.2"
PYTHON_VERSION="3.11"
CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ENV_FILE="/etc/autopulse/airflow.env"
PASSWORDS_FILE="${AIRFLOW_HOME}/simple_auth_manager_passwords.json"
SNOWFLAKE_ACCOUNT="RHELJEO-OZ82635"
SNOWFLAKE_KEY="/home/${APP_USER}/.snowflake/rsa_key.p8"

SWAP_MB=4096
echo "== 0/8  Swap file (${SWAP_MB} MB of disk used as extra memory when RAM runs out)"
if [ -f /swapfile ] && [ "$(stat -c %s /swapfile)" -lt $((SWAP_MB * 1024 * 1024)) ]; then
    swapoff /swapfile || true            # smaller swap file from an earlier setup: replace it
    rm -f /swapfile
    sed -i '/^\/swapfile /d' /etc/fstab
fi
if [ ! -f /swapfile ]; then
    dd if=/dev/zero of=/swapfile bs=1M count=${SWAP_MB} status=none
    chmod 600 /swapfile
    mkswap /swapfile
    echo "/swapfile none swap sw 0 0" >> /etc/fstab
fi
swapon /swapfile 2>/dev/null || true

echo "== 1/8  System packages: Python 3.11, PostgreSQL 15, OpenSSL"
dnf install -y python3.11 python3.11-pip postgresql15 postgresql15-server openssl

echo "== 2/8  Passwords and secrets (created once, kept in ${ENV_FILE})"
mkdir -p /etc/autopulse
# Read a value saved by an earlier run of this script, or create a new random one.
saved_or_new() {
    local value=""
    [ -f "${ENV_FILE}" ] && value="$(grep "^$1=" "${ENV_FILE}" | cut -d= -f2- || true)"
    echo "${value:-$(openssl rand -hex 16)}"
}
DB_PASSWORD="$(saved_or_new AUTOPULSE_DB_PASSWORD)"
ADMIN_PASSWORD="$(saved_or_new AUTOPULSE_ADMIN_PASSWORD)"
JWT_SECRET="$(saved_or_new AIRFLOW__API_AUTH__JWT_SECRET)"     # signs the tokens tasks use to talk to the API server
API_SECRET_KEY="$(saved_or_new AIRFLOW__API__SECRET_KEY)"

echo "== 3/8  PostgreSQL: Airflow's database"
if [ ! -f /var/lib/pgsql/data/PG_VERSION ]; then
    postgresql-setup --initdb
fi
HBA=/var/lib/pgsql/data/pg_hba.conf
if ! grep -q "# autopulse" "${HBA}"; then
    # Let the 'airflow' database user log in with its password from this machine only.
    sed -i '1i host    airflow    airflow    127.0.0.1/32    scram-sha-256    # autopulse' "${HBA}"
fi
systemctl enable --now postgresql
systemctl reload postgresql
cd /tmp
sudo -u postgres psql -v ON_ERROR_STOP=1 -q -c "DO \$\$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow LOGIN PASSWORD '${DB_PASSWORD}';
    ELSE
        ALTER ROLE airflow PASSWORD '${DB_PASSWORD}';    -- keep it in step with ${ENV_FILE}
    END IF; END \$\$;"
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname = 'airflow'" | grep -q 1; then
    sudo -u postgres createdb -O airflow airflow
fi

echo "== 4/8  Project code from ${CODE_S3}/"
mkdir -p "${APP_DIR}"
aws s3 sync "${CODE_S3}/" "${APP_DIR}/" --delete --exclude "data/*"
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

echo "== 5/8  Airflow ${AIRFLOW_VERSION} (with the AWS and Snowflake add-ons); first time ~5 min"
PIP_TEMP_DIR="/var/tmp/autopulse-pip"
mkdir -p "${PIP_TEMP_DIR}" && chown "${APP_USER}:${APP_USER}" "${PIP_TEMP_DIR}"
# Upgrading from Airflow 2: stop it and start from a fresh Python environment.
if [ -x "${VENV_DIR}/bin/airflow" ] && ! sudo -u "${APP_USER}" "${VENV_DIR}/bin/pip" show apache-airflow 2>/dev/null \
        | grep -q "^Version: ${AIRFLOW_VERSION}$"; then
    echo "   Another Airflow version is installed: stopping it and replacing its environment"
    systemctl stop airflow-webserver airflow-scheduler airflow-dag-processor airflow-api-server 2>/dev/null || true
    rm -rf "${VENV_DIR}"
fi
if [ ! -d "${VENV_DIR}" ]; then
    sudo -u "${APP_USER}" python3.11 -m venv "${VENV_DIR}"
fi
sudo -u "${APP_USER}" env TMPDIR="${PIP_TEMP_DIR}" "${VENV_DIR}/bin/pip" install --quiet --upgrade pip
# The constraints file pins every library to versions tested with this Airflow release.
sudo -u "${APP_USER}" env TMPDIR="${PIP_TEMP_DIR}" "${VENV_DIR}/bin/pip" install --quiet --no-cache-dir \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-standard apache-airflow-providers-amazon apache-airflow-providers-snowflake \
    boto3 pyarrow \
    --constraint "${CONSTRAINTS_URL}"
rm -rf "${PIP_TEMP_DIR}"

echo "== 6/8  Snowflake key pair (Airflow logs in to Snowflake with a key, not a password)"
if [ ! -f "${SNOWFLAKE_KEY}" ]; then
    mkdir -p "$(dirname "${SNOWFLAKE_KEY}")"
    openssl genrsa 2048 2>/dev/null | openssl pkcs8 -topk8 -inform PEM -nocrypt -out "${SNOWFLAKE_KEY}"
fi
chown -R "${APP_USER}:${APP_USER}" "$(dirname "${SNOWFLAKE_KEY}")"
chmod 700 "$(dirname "${SNOWFLAKE_KEY}")"
chmod 600 "${SNOWFLAKE_KEY}"

echo "== 7/8  Airflow settings (${ENV_FILE}) and database"
cat > "${ENV_FILE}" <<EOF
# AutoPulse Airflow settings, read by the Airflow services (written by setup_airflow_host.sh).
AUTOPULSE_DB_PASSWORD=${DB_PASSWORD}
AUTOPULSE_ADMIN_PASSWORD=${ADMIN_PASSWORD}
AIRFLOW_HOME=${AIRFLOW_HOME}
AIRFLOW__CORE__DAGS_FOLDER=${APP_DIR}/orchestration/airflow_dags
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__PARALLELISM=3
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DEFAULT_TIMEZONE=America/New_York
AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=${PASSWORDS_FILE}
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080/execution/
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${DB_PASSWORD}@127.0.0.1:5432/airflow
AIRFLOW__API__PORT=8080
AIRFLOW__API__WORKERS=1
AIRFLOW__API__EXPOSE_CONFIG=False
AIRFLOW__API__SECRET_KEY=${API_SECRET_KEY}
AIRFLOW__API_AUTH__JWT_SECRET=${JWT_SECRET}
AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=60
AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=60
AWS_DEFAULT_REGION=us-east-1
AIRFLOW_CONN_SNOWFLAKE_AUTOPULSE='{"conn_type": "snowflake", "login": "AUTOPULSE_AIRFLOW", "schema": "CORE", "extra": {"account": "${SNOWFLAKE_ACCOUNT}", "warehouse": "AUTOPULSE_WH", "database": "AUTOPULSE", "role": "AUTOPULSE_MONITOR", "private_key_file": "${SNOWFLAKE_KEY}"}}'
EOF
chown root:"${APP_USER}" "${ENV_FILE}"
chmod 640 "${ENV_FILE}"
mkdir -p "${AIRFLOW_HOME}" && chown "${APP_USER}:${APP_USER}" "${AIRFLOW_HOME}"

run_airflow() {
    sudo -u "${APP_USER}" bash -c "set -a; source ${ENV_FILE}; set +a; cd ${AIRFLOW_HOME}; ${VENV_DIR}/bin/airflow $*"
}
run_airflow db migrate       # creates the tables, or upgrades them from Airflow 2

# Airflow 3's built-in login ("simple auth manager"): the users are listed in
# AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS, their passwords in this JSON file.
echo "{\"admin\": \"${ADMIN_PASSWORD}\"}" > "${PASSWORDS_FILE}"
chown "${APP_USER}:${APP_USER}" "${PASSWORDS_FILE}"
chmod 600 "${PASSWORDS_FILE}"

echo "== 8/8  Services: install, enable at boot, (re)start"
# Airflow 2's web server is replaced by the API server in Airflow 3.
if [ -f /etc/systemd/system/airflow-webserver.service ]; then
    systemctl disable --now airflow-webserver.service || true
    rm -f /etc/systemd/system/airflow-webserver.service
fi
SERVICES="airflow-api-server.service airflow-dag-processor.service airflow-scheduler.service"
cp "${APP_DIR}"/infrastructure/ec2_airflow_host/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable ${SERVICES}
systemctl restart ${SERVICES}

PUBLIC_IP="$(curl -s -m 3 -H "X-aws-ec2-metadata-token: $(curl -s -m 3 -X PUT http://169.254.169.254/latest/api/token \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/public-ipv4 || true)"
echo
echo "Done. Airflow web page (about 1 minute after this): http://${PUBLIC_IP:-<instance public IP>}:8080"
echo "  username: admin"
echo "  password: ${ADMIN_PASSWORD}"
echo
echo "Snowflake public key (paste into snowflake/10_airflow_monitoring_user.sql, RSA_PUBLIC_KEY):"
openssl rsa -in "${SNOWFLAKE_KEY}" -pubout 2>/dev/null | grep -v -- '-----' | tr -d '\n'
echo
echo
echo "Check the services:  systemctl status airflow-api-server airflow-dag-processor airflow-scheduler --no-pager"
