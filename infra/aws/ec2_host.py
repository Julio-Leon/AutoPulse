"""
AutoPulse | Manage the EC2 host (Kafka, simulator, Spark streaming, Airflow).

    python infra/aws/ec2_host.py launch      create the instance (first time only)
    python infra/aws/ec2_host.py status      state, IDs, and how to open Airflow
    python infra/aws/ec2_host.py update      run the latest deployed code on the host
                                             (run infra/aws/deploy.py first)
    python infra/aws/ec2_host.py logs        last lines of bootstrap/update logs + container status
    python infra/aws/ec2_host.py logs --service streaming --lines 80
                                             one service's log (kafka, simulator, streaming,
                                             airflow-scheduler, airflow-webserver, postgres)
    python infra/aws/ec2_host.py password    print the Airflow admin password
    python infra/aws/ec2_host.py reset-telemetry
                                             start telemetry over: stop streaming, delete telemetry
                                             Bronze/Silver/quarantine + checkpoints, start streaming
                                             (Kafka still holds the last 7 days, so events are re-read)
    python infra/aws/ec2_host.py stop        stop the instance (saves cost; data is kept)
    python infra/aws/ec2_host.py start       start it again (services restart automatically)
    python infra/aws/ec2_host.py terminate   delete the instance (asks for confirmation)

The host has no inbound ports. Shell and the Airflow UI are reached through
AWS Systems Manager (needs the Session Manager plugin for the AWS CLI).
Prerequisite: `python infra/aws/deploy.py` has run (it creates the instance
role and uploads the code bundle the host downloads at boot).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SCRIPT = PROJECT_ROOT / "deploy" / "ec2" / "bootstrap.sh"

NAME_TAG = "autopulse-host"
SECURITY_GROUP = "autopulse-host-sg"
INSTANCE_PROFILE = "AutoPulseEc2HostProfile"
INSTANCE_TYPE = "t3.large"          # 2 vCPU / 8 GB: Kafka + Spark + Airflow
VOLUME_GB = 30
AL2023_AMI_PARAMETER = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
LIVE_STATES = ["pending", "running", "stopping", "stopped"]

# Everything derived from the telemetry stream (prefixes in the bucket).
# No trailing slash, so Glue's "<prefix>_$folder$" marker objects are removed too.
TELEMETRY_PREFIXES = [
    "bronze/vehicle_telemetry",
    "silver/vehicle_telemetry",
    "quarantine/vehicle_telemetry",
    "_checkpoints/vehicle_telemetry_bronze",
    "_checkpoints/vehicle_telemetry_silver",
]
COMPOSE_DIR = "/opt/autopulse/app/deploy/ec2"


def log(message: str) -> None:
    print(f"==> {message}", flush=True)


class Host:
    def __init__(self, session, bucket: str, region: str):
        self.bucket = bucket
        self.region = region
        self.ec2 = session.client("ec2", region_name=region)
        self.ssm = session.client("ssm", region_name=region)
        self.s3 = session.client("s3", region_name=region)

    # ---------------------------------------------------------------- lookup
    def find(self) -> dict | None:
        reservations = self.ec2.describe_instances(Filters=[
            {"Name": "tag:Name", "Values": [NAME_TAG]},
            {"Name": "instance-state-name", "Values": LIVE_STATES},
        ])["Reservations"]
        instances = [i for r in reservations for i in r["Instances"]]
        return instances[0] if instances else None

    def require(self) -> dict:
        instance = self.find()
        if not instance:
            sys.exit(f"No '{NAME_TAG}' instance found. Create it with: python infra/aws/ec2_host.py launch")
        return instance

    # ---------------------------------------------------------------- launch
    def _ami(self) -> str:
        try:
            return self.ssm.get_parameter(Name=AL2023_AMI_PARAMETER)["Parameter"]["Value"]
        except Exception:
            images = self.ec2.describe_images(
                Owners=["amazon"],
                Filters=[{"Name": "name", "Values": ["al2023-ami-2023.*-kernel-*-x86_64"]},
                         {"Name": "state", "Values": ["available"]}],
            )["Images"]
            if not images:
                sys.exit("Could not find an Amazon Linux 2023 image.")
            return max(images, key=lambda image: image["CreationDate"])["ImageId"]

    def _subnet(self, subnet_id: str | None) -> tuple[str, str]:
        if subnet_id:
            subnet = self.ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]
            return subnet["SubnetId"], subnet["VpcId"]
        vpcs = self.ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
        if not vpcs:
            sys.exit("No default VPC in this region. Pass --subnet-id <a public subnet>.")
        vpc_id = vpcs[0]["VpcId"]
        subnets = self.ec2.describe_subnets(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default-for-az", "Values": ["true"]},
        ])["Subnets"]
        if not subnets:
            sys.exit(f"No default subnets in {vpc_id}. Pass --subnet-id.")
        subnet = sorted(subnets, key=lambda s: s["AvailabilityZone"])[0]
        return subnet["SubnetId"], vpc_id

    def _security_group(self, vpc_id: str) -> str:
        groups = self.ec2.describe_security_groups(Filters=[
            {"Name": "group-name", "Values": [SECURITY_GROUP]},
            {"Name": "vpc-id", "Values": [vpc_id]},
        ])["SecurityGroups"]
        if groups:
            return groups[0]["GroupId"]
        # No inbound rules: access is only through Systems Manager.
        group_id = self.ec2.create_security_group(
            GroupName=SECURITY_GROUP,
            Description="AutoPulse host - no inbound; access via SSM",
            VpcId=vpc_id,
        )["GroupId"]
        log(f"Created security group {SECURITY_GROUP} ({group_id}) with no inbound rules")
        return group_id

    def launch(self, subnet_id: str | None, instance_type: str) -> None:
        existing = self.find()
        if existing:
            log(f"Instance already exists: {existing['InstanceId']} ({existing['State']['Name']}). Nothing to do.")
            return

        user_data = (BOOTSTRAP_SCRIPT.read_text(encoding="utf-8")
                     .replace("\r\n", "\n")
                     .replace("__BUCKET__", self.bucket)
                     .replace("__REGION__", self.region))
        subnet, vpc_id = self._subnet(subnet_id)
        params = dict(
            ImageId=self._ami(),
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            IamInstanceProfile={"Name": INSTANCE_PROFILE},
            UserData=user_data,
            NetworkInterfaces=[{
                "DeviceIndex": 0,
                "SubnetId": subnet,
                "Groups": [self._security_group(vpc_id)],
                "AssociatePublicIpAddress": True,     # outbound internet (Docker images, APIs)
            }],
            # Containers reach the instance role through IMDSv2; that needs 2 hops.
            MetadataOptions={"HttpTokens": "required", "HttpPutResponseHopLimit": 2, "HttpEndpoint": "enabled"},
            BlockDeviceMappings=[{
                "DeviceName": "/dev/xvda",
                "Ebs": {"VolumeSize": VOLUME_GB, "VolumeType": "gp3", "DeleteOnTermination": True},
            }],
            TagSpecifications=[
                {"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": NAME_TAG}, {"Key": "project", "Value": "autopulse"}]},
                {"ResourceType": "volume", "Tags": [{"Key": "Name", "Value": NAME_TAG}, {"Key": "project", "Value": "autopulse"}]},
            ],
        )

        # A just-created instance profile can take a few seconds to be usable.
        for attempt in range(1, 13):
            try:
                instance = self.ec2.run_instances(**params)["Instances"][0]
                break
            except self.ec2.exceptions.ClientError as error:
                if "iamInstanceProfile" not in str(error) or attempt == 12:
                    raise
                log("Waiting for the instance profile to become usable...")
                time.sleep(10)

        instance_id = instance["InstanceId"]
        log(f"Launched {instance_id} ({instance_type}) in {subnet}. Waiting for it to run...")
        self.ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        log("Running. First-boot setup (Docker, images, services) takes about 5-10 minutes.")
        log("Check progress with:  python infra/aws/ec2_host.py logs")

    # ---------------------------------------------------------------- SSM commands
    def run(self, instance_id: str, commands: list[str], timeout: int = 900) -> str:
        command_id = self.ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
        )["Command"]["CommandId"]
        deadline = time.time() + timeout + 60
        while time.time() < deadline:
            time.sleep(5)
            try:
                result = self.ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
            except self.ssm.exceptions.InvocationDoesNotExist:
                continue
            if result["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
                output = result.get("StandardOutputContent", "") + result.get("StandardErrorContent", "")
                if result["Status"] != "Success":
                    print(output)
                    sys.exit(f"Command {result['Status']} on {instance_id}")
                return output
        sys.exit("Timed out waiting for the command to finish.")

    def require_running(self) -> str:
        instance = self.require()
        if instance["State"]["Name"] != "running":
            sys.exit(f"Instance {instance['InstanceId']} is {instance['State']['Name']}. Start it first.")
        return instance["InstanceId"]

    # ---------------------------------------------------------------- actions
    def status(self) -> None:
        instance = self.find()
        if not instance:
            print(f"No '{NAME_TAG}' instance. Create it with: python infra/aws/ec2_host.py launch")
            return
        iid = instance["InstanceId"]
        print(f"Instance : {iid}")
        print(f"State    : {instance['State']['Name']}")
        print(f"Type     : {instance['InstanceType']}")
        print(f"Launched : {instance['LaunchTime']}")
        print("\nShell on the host:")
        print(f"  aws ssm start-session --target {iid}")
        print("\nAirflow UI (then open http://localhost:8080, user 'admin'):")
        print(f"  aws ssm start-session --target {iid} --document-name AWS-StartPortForwardingSession "
              "--parameters 'portNumber=8080,localPortNumber=8080'")
        print("  password:  python infra/aws/ec2_host.py password")

    def update(self) -> None:
        iid = self.require_running()
        log(f"Updating {iid} to the latest deployed bundle...")
        output = self.run(iid, ["bash /opt/autopulse/app/deploy/ec2/update.sh"])
        print("\n".join(output.splitlines()[-25:]))

    def logs(self, service: str | None = None, lines: int = 10) -> None:
        iid = self.require_running()
        if service:
            print(self.run(iid, [
                f"cd /opt/autopulse/app/deploy/ec2 && docker compose ps {service} --format '{{{{.Service}}}}: {{{{.Status}}}}'",
                f"cd /opt/autopulse/app/deploy/ec2 && docker compose logs --no-color --tail {int(lines)} {service}",
            ], timeout=120))
            return
        print(self.run(iid, [
            "echo '--- bootstrap (last 20 lines)'; tail -20 /var/log/autopulse-bootstrap.log 2>/dev/null || echo 'not started yet'",
            "echo '--- update (last 10 lines)'; tail -10 /var/log/autopulse-update.log 2>/dev/null || echo 'no updates yet'",
            "echo '--- containers'; cd /opt/autopulse/app/deploy/ec2 2>/dev/null && docker compose ps --format 'table {{.Service}}\\t{{.Status}}' || true",
            "echo '--- streaming (last 10 lines)'; cd /opt/autopulse/app/deploy/ec2 2>/dev/null && docker compose logs --tail 10 streaming || true",
        ], timeout=120))

    def password(self) -> None:
        iid = self.require_running()
        output = self.run(iid, ["grep '^AIRFLOW_ADMIN_PASSWORD=' /opt/autopulse/autopulse.env | cut -d= -f2-"], timeout=60)
        print(f"Airflow user: admin\nPassword    : {output.strip()}")

    def delete_prefix(self, prefix: str) -> int:
        deleted = 0
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            for start in range(0, len(keys), 1000):
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys[start:start + 1000], "Quiet": True})
            deleted += len(keys)
        return deleted

    def reset_telemetry(self, yes: bool) -> None:
        iid = self.require_running()
        print("This deletes all telemetry Bronze, Silver and quarantine data and both streaming")
        print(f"checkpoints in s3://{self.bucket}/. Kafka on the host keeps the last 7 days of events,")
        print("so the stream re-reads them. Snowflake data (if any) is NOT deleted.")
        if not yes and input("Type RESET to continue: ").strip() != "RESET":
            sys.exit("Cancelled.")

        log("Stopping the streaming service...")
        self.run(iid, [f"cd {COMPOSE_DIR} && docker compose stop streaming"], timeout=180)
        for prefix in TELEMETRY_PREFIXES:
            log(f"Deleted {self.delete_prefix(prefix):>5} objects under s3://{self.bucket}/{prefix}")
        log("Starting the streaming service...")
        self.run(iid, [f"cd {COMPOSE_DIR} && docker compose start streaming"], timeout=180)
        log("Done. New Bronze files appear within ~2 minutes; the next telemetry DAG run builds Silver.")

    def stop(self) -> None:
        iid = self.require()["InstanceId"]
        self.ec2.stop_instances(InstanceIds=[iid])
        log(f"Stopping {iid}. Data is kept; start it again with: python infra/aws/ec2_host.py start")

    def start(self) -> None:
        iid = self.require()["InstanceId"]
        self.ec2.start_instances(InstanceIds=[iid])
        self.ec2.get_waiter("instance_running").wait(InstanceIds=[iid])
        log(f"{iid} is running; services restart automatically within a few minutes.")

    def terminate(self, yes: bool) -> None:
        iid = self.require()["InstanceId"]
        if not yes and input(f"Type the instance ID to delete it ({iid}): ").strip() != iid:
            sys.exit("Cancelled.")
        self.ec2.terminate_instances(InstanceIds=[iid])
        log(f"Terminating {iid}. Kafka/Airflow data on the host is deleted; S3 and Snowflake are not affected.")


def main(argv=None, session=None) -> None:
    parser = argparse.ArgumentParser(description="Manage the AutoPulse EC2 host.")
    parser.add_argument("action", choices=["launch", "status", "update", "logs", "password",
                                           "reset-telemetry", "stop", "start", "terminate"])
    parser.add_argument("--bucket", default=os.getenv("BUCKET", "autopulse-julio-leon"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--subnet-id", help="Public subnet to launch in (default: a default-VPC subnet)")
    parser.add_argument("--instance-type", default=INSTANCE_TYPE)
    parser.add_argument("--yes", action="store_true", help="Skip the terminate / reset-telemetry confirmation")
    parser.add_argument("--service", help="logs: show one service's log")
    parser.add_argument("--lines", type=int, default=10, help="logs: number of lines (default 10)")
    args = parser.parse_args(argv)

    if session is None:
        try:
            import boto3
        except ImportError:
            sys.exit("boto3 is not installed. Run:  pip install boto3")
        session = boto3.Session(region_name=args.region)

    host = Host(session, args.bucket, args.region)
    if args.action == "launch":
        host.launch(args.subnet_id, args.instance_type)
        host.status()
    elif args.action == "terminate":
        host.terminate(args.yes)
    elif args.action == "logs":
        host.logs(args.service, args.lines)
    elif args.action == "reset-telemetry":
        host.reset_telemetry(args.yes)
    else:
        getattr(host, args.action)()


if __name__ == "__main__":
    main()
