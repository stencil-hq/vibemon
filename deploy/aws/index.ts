/**
 * vibevmm orchestration layer on AWS.
 *
 * Topology (N×M, brokerless — mirrors vmond/src/orch):
 *
 *   clients ──▶ scheduler EC2 (vmon sched, EIP each) ──direct gRPC──▶ worker ASG (vmon serve)
 *                     │  in-memory worker table                              │ self-published heartbeats
 *                     └────────── follows stream ──────────▶ state VM ◀──────┘
 *                                                     (Redis 6379 + Postgres 5432)
 *
 * Cost stance (deliberate): no RDS, no ElastiCache, no NAT gateways, no NLB by
 * default. One small VM runs both Redis (orch state bus) and Postgres (vmond
 * cluster substrate) — the orch layer treats Redis as reconstructible cache,
 * so a single box is an accepted trade, not an oversight.
 *
 * Autoscaling: the vmon sched leader computes desired capacity (HPA-like) and
 * drives THIS stack's worker ASG through the scale hooks:
 *   scale-up.sh   → aws autoscaling set-desired-capacity $VMON_SCALE_DESIRED
 *   scale-down.sh → terminate-instance-in-auto-scaling-group for each
 *                   $VMON_IDLE_WIDS entry (drained AND empty workers only)
 * Worker ids ARE EC2 instance ids (VMON_ORCH_ID=instance-id), which is what
 * makes the terminate mapping trivial. The ASG has scale-in protection on so
 * AWS never picks victims itself.
 */

import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import { gzipSync } from "node:zlib";
import * as random from "@pulumi/random";

const config = new pulumi.Config();

/** S3 URI preferred for IAM-authenticated binary downloads by autoscaled instances. */
const binaryS3Uri = config.get("binaryS3Uri");
/** HTTPS fallback for deployments that host the binary outside S3. */
const binaryUrl = binaryS3Uri ? undefined : config.require("binaryUrl");
/** Optional S3 tarball with kernel/agent assets. */
const assetsS3Uri = config.get("assetsS3Uri");
/** HTTPS fallback for deployments that host assets outside S3. */
const assetsUrl = assetsS3Uri ? undefined : config.get("assetsUrl");
/** CIDR allowed to reach schedulers (and worker endpoints for direct dials). */
const allowedCidr = config.get("allowedCidr") ?? "0.0.0.0/0";
/** Worker fleet bounds; the vmon autoscaler moves desired capacity in [min, max]. */
const workerMin = config.getNumber("workerMin") ?? 1;
const workerMax = config.getNumber("workerMax") ?? 4;
/** Guest architecture of the fleet; must match the vmon binary at binaryUrl. */
const arch = config.get("arch") ?? "arm64";
/** EC2 exposes nested KVM on supported x86 virtual families; Graviton still requires metal. */
const workerInstanceType =
  config.get("workerInstanceType") ??
  (arch === "arm64" ? "c7g.metal" : "m7i.2xlarge");
const workerIsMetal = workerInstanceType.endsWith(".metal");
const schedulerInstanceType =
  config.get("schedulerInstanceType") ??
  (arch === "arm64" ? "t4g.small" : "t3.small");
const stateInstanceType =
  config.get("stateInstanceType") ??
  (arch === "arm64" ? "t4g.small" : "t3.small");
const schedulerCount = config.getNumber("schedulerCount") ?? 1;
/** Per-worker admission cap (0 = memory-bound only). */
const maxSandboxesPerWorker = config.getNumber("maxSandboxesPerWorker") ?? 0;
/** Preallocated per-worker TAP/network slots for create-path admission. */
const netSlots = config.getNumber("netSlots") ?? 256;
/** Autoscaler target memory utilization (0, 1]. */
const targetUtil = config.getNumber("targetUtil") ?? 0.7;

const workerPort = 8000;
const schedPort = 8100;
const dashboardPort = 8080;

function s3ObjectArn(uri: string): string {
  const match = /^s3:\/\/([^/]+)\/(.+)$/.exec(uri);
  if (!match) {
    throw new Error(`invalid S3 artifact URI: ${uri}`);
  }
  return `arn:aws:s3:::${match[1]}/${match[2]}`;
}

function artifactReadPolicy(resources: string[]): string {
  return JSON.stringify({
    Version: "2012-10-17",
    Statement: [
      { Effect: "Allow", Action: "s3:GetObject", Resource: resources },
    ],
  });
}

const ec2AssumeRolePolicy = JSON.stringify({
  Version: "2012-10-17",
  Statement: [
    {
      Effect: "Allow",
      Principal: { Service: "ec2.amazonaws.com" },
      Action: "sts:AssumeRole",
    },
  ],
});
const workerArtifactArns: string[] = [];
if (binaryS3Uri) {
  workerArtifactArns.push(s3ObjectArn(binaryS3Uri));
}
if (assetsS3Uri) {
  workerArtifactArns.push(s3ObjectArn(assetsS3Uri));
}

const region = aws.getRegionOutput().name;
const ami = aws.ssm.getParameterOutput({
  name: `/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${arch === "arm64" ? "arm64" : "x86_64"}`,
}).value;

// ── Secrets ─────────────────────────────────────────────────────────────────
const apiToken = new random.RandomPassword("api-token", {
  length: 40,
  special: false,
});
const workerToken = new random.RandomPassword("worker-token", {
  length: 40,
  special: false,
});
const redisPassword = new random.RandomPassword("redis-password", {
  length: 32,
  special: false,
});
const pgPassword = new random.RandomPassword("pg-password", {
  length: 32,
  special: false,
});

// ── Network: VPC with public subnets only (no NAT $$$) ─────────────────────
const vpc = new aws.ec2.Vpc("orch", {
  cidrBlock: "10.42.0.0/16",
  enableDnsSupport: true,
  enableDnsHostnames: true,
  tags: { Name: "vibevmm-orch" },
});
const igw = new aws.ec2.InternetGateway("orch", { vpcId: vpc.id });
const routeTable = new aws.ec2.RouteTable("orch", {
  vpcId: vpc.id,
  routes: [{ cidrBlock: "0.0.0.0/0", gatewayId: igw.id }],
});
const zones = aws.getAvailabilityZonesOutput({ state: "available" });
const subnets = [0, 1].map((index) => {
  const subnet = new aws.ec2.Subnet(`orch-${index}`, {
    vpcId: vpc.id,
    cidrBlock: `10.42.${index * 16}.0/20`,
    availabilityZone: zones.names[index],
    mapPublicIpOnLaunch: true,
    tags: { Name: `vibevmm-orch-${index}` },
  });
  new aws.ec2.RouteTableAssociation(`orch-${index}`, {
    subnetId: subnet.id,
    routeTableId: routeTable.id,
  });
  return subnet;
});
const subnetIds = subnets.map((subnet) => subnet.id);

// ── Security groups ─────────────────────────────────────────────────────────
const schedSg = new aws.ec2.SecurityGroup("sched", {
  vpcId: vpc.id,
  description: "vibevmm schedulers",
  ingress: [
    {
      protocol: "tcp",
      fromPort: schedPort,
      toPort: schedPort,
      cidrBlocks: [allowedCidr],
      description: "scheduler gRPC",
    },
    {
      protocol: "tcp",
      fromPort: dashboardPort,
      toPort: dashboardPort,
      cidrBlocks: [allowedCidr],
      description: "scheduler dashboard",
    },
  ],
  egress: [
    { protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] },
  ],
});
const workerSg = new aws.ec2.SecurityGroup(
  "worker",
  {
    vpcId: vpc.id,
    description: "vibevmm workers",
    ingress: [
      {
        protocol: "tcp",
        fromPort: workerPort,
        toPort: workerPort,
        securityGroups: [schedSg.id],
        description: "scheduler-forwarded gRPC",
      },
      {
        protocol: "tcp",
        fromPort: workerPort,
        toPort: workerPort,
        cidrBlocks: [allowedCidr],
        description: "direct endpoint dials (view.endpoint)",
      },
    ],
    egress: [
      { protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] },
    ],
  },
  { ignoreChanges: ["ingress"] },
);
const stateSg = new aws.ec2.SecurityGroup("state", {
  vpcId: vpc.id,
  description: "vibevmm state VM (redis + postgres)",
  ingress: [
    {
      protocol: "tcp",
      fromPort: 6379,
      toPort: 6379,
      securityGroups: [schedSg.id, workerSg.id],
      description: "redis",
    },
    {
      protocol: "tcp",
      fromPort: 5432,
      toPort: 5432,
      securityGroups: [schedSg.id, workerSg.id],
      description: "postgres",
    },
  ],
  egress: [
    { protocol: "-1", fromPort: 0, toPort: 0, cidrBlocks: ["0.0.0.0/0"] },
  ],
});

// ── State VM: Redis + Postgres ─────────────────────────────────────────────
// The scheduler serves the dashboard alongside its gRPC endpoint.
const stateUserData = pulumi.interpolate`#!/bin/bash
set -euxo pipefail
dnf install -y redis6 postgresql15-server

# redis: bind to the VPC, password auth
sed -i 's/^bind .*/bind 0.0.0.0 -::1/' /etc/redis6/redis6.conf
sed -i 's/^protected-mode yes/protected-mode no/' /etc/redis6/redis6.conf
echo 'requirepass ${redisPassword.result}' >> /etc/redis6/redis6.conf
systemctl enable --now redis6

# postgres: VPC-local scram auth for the vmon role
postgresql-setup --initdb
echo "listen_addresses = '*'" >> /var/lib/pgsql/data/postgresql.conf
echo "host all vmon 10.42.0.0/16 scram-sha-256" >> /var/lib/pgsql/data/pg_hba.conf
systemctl enable --now postgresql
sudo -u postgres psql -c "CREATE ROLE vmon LOGIN PASSWORD '${pgPassword.result}';"
sudo -u postgres createdb -O vmon vmon

`;
const stateUserDataBase64 = stateUserData.apply((script) =>
  gzipSync(Buffer.from(script)).toString("base64"),
);
const stateInstance = new aws.ec2.Instance("state", {
  ami,
  instanceType: stateInstanceType,
  subnetId: subnetIds[0],
  vpcSecurityGroupIds: [stateSg.id],
  userDataBase64: stateUserDataBase64,
  userDataReplaceOnChange: true,
  metadataOptions: { httpTokens: "required" },
  rootBlockDevice: { volumeSize: 20, volumeType: "gp3" },
  tags: { Name: "vibevmm-state" },
});

const redisUrl = pulumi.interpolate`redis://:${redisPassword.result}@${stateInstance.privateIp}:6379`;
const postgresUrl = pulumi.interpolate`postgres://vmon:${pgPassword.result}@${stateInstance.privateIp}:5432/vmon`;

// ── Workers: nested-KVM ASG, scale-in protected, vmon-driven ───────────────
const binaryInstall = binaryS3Uri
  ? `aws s3 cp "${binaryS3Uri}" /usr/local/bin/vmon`
  : `curl -fsSL "${binaryUrl}" -o /usr/local/bin/vmon`;
const assetsSnippet = assetsS3Uri
  ? `mkdir -p /var/lib/vmon/assets
aws s3 cp "${assetsS3Uri}" - | tar -xz -C /var/lib/vmon/assets`
  : assetsUrl
    ? `mkdir -p /var/lib/vmon/assets
curl -fsSL "${assetsUrl}" | tar -xz -C /var/lib/vmon/assets`
    : "true # no assets tarball configured";

const workerUserData = pulumi.interpolate`#!/bin/bash
set -euxo pipefail
# \`nftables\` carries the \`nft\` binary the network broker invokes;
# \`iptables-nft\` only supplies the iptables compatibility wrappers.
dnf install -y iptables-nft nftables

getent group vmon >/dev/null || groupadd --system vmon
if ! id -u vmon >/dev/null 2>&1; then
  useradd --system --gid vmon --home-dir /var/lib/vmon --shell /sbin/nologin --comment "Vibemon worker" vmon
fi
if getent group kvm >/dev/null; then
  usermod --append --groups kvm vmon
fi
install -d -o vmon -g vmon -m 0700 /var/lib/vmon
install -d -o root -g vmon -m 0750 /etc/vmon

${binaryInstall}
chmod 0755 /usr/local/bin/vmon
${assetsSnippet}

# OCI image pipeline tools (AL2023 packages neither; static release builds).
# skopeo has no official static artifact — lework/skopeo-binary is the
# community-maintained build; pin versions and swap for your own mirror in
# security-sensitive deployments.
curl -fsSL https://github.com/lework/skopeo-binary/releases/download/v1.16.1/skopeo-linux-amd64 -o /usr/local/bin/skopeo
curl -fsSL https://github.com/opencontainers/umoci/releases/download/v0.4.7/umoci.amd64 -o /usr/local/bin/umoci
chmod 0755 /usr/local/bin/skopeo /usr/local/bin/umoci
mkdir -p /etc/containers
printf 'unqualified-search-registries = ["docker.io"]\nshort-name-mode = "permissive"\n' > /etc/containers/registries.conf
cat > /etc/containers/policy.json <<'EOF'
{
  "default": [{ "type": "insecureAcceptAnything" }]
}
EOF

IMDS_TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
INSTANCE_ID=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

cat > /etc/vmon/worker.env <<EOF
VMON_HOME=/var/lib/vmon
VMON_API_TOKEN=${workerToken.result}
VMON_ORCH_REDIS=${redisUrl}
VMON_ORCH_ID=$INSTANCE_ID
VMON_ORCH_URL=http://$PRIVATE_IP:${workerPort}
VMON_ORCH_MAX_SANDBOXES=${maxSandboxesPerWorker}
VMON_NETWORK_BROKER_SOCKET=/run/vmon/broker.sock
VMON_NET_SLOTS=${netSlots}
# Postgres is provisioned for cluster_mode=production, which additionally
# requires s3_endpoint/s3_bucket credentials; workers default to single-node.
VMON_POSTGRES_URL=${postgresUrl}
EOF

# Bundle-provided guest agent / kernel override the auto-provisioned ones.
GUEST_ARCH=$(uname -m)
if [ -f "/var/lib/vmon/assets/vmon-agent-$GUEST_ARCH" ]; then
  chmod 0755 "/var/lib/vmon/assets/vmon-agent-$GUEST_ARCH"
  echo "VMON_AGENT=/var/lib/vmon/assets/vmon-agent-$GUEST_ARCH" >> /etc/vmon/worker.env
fi
for kernel in Image bzImage; do
  if [ -f "/var/lib/vmon/assets/$kernel-$GUEST_ARCH" ]; then
    echo "VMON_KERNEL=/var/lib/vmon/assets/$kernel-$GUEST_ARCH" >> /etc/vmon/worker.env
  fi
done

chown -R vmon:vmon /var/lib/vmon
chown root:vmon /etc/vmon/worker.env
chmod 0640 /etc/vmon/worker.env

VMON_UID=$(id -u vmon)
cat > /etc/systemd/system/vmon-netbroker.service <<EOF
[Unit]
Description=vibevmm privileged network broker
After=network-online.target
Before=vmon-worker.service
Wants=network-online.target

[Service]
RuntimeDirectory=vmon
ExecStart=/usr/local/bin/vmon net-broker --socket /run/vmon/broker.sock --owner-uid $VMON_UID
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/vmon-worker.service <<'EOF'
[Unit]
Description=vibevmm orchestration worker
After=network-online.target vmon-netbroker.service
Wants=network-online.target
Requires=vmon-netbroker.service

[Service]
User=vmon
Group=vmon
WorkingDirectory=/var/lib/vmon
EnvironmentFile=/etc/vmon/worker.env
ExecStart=/usr/local/bin/vmon serve --host 0.0.0.0 --port ${workerPort}
Restart=always
RestartSec=2
LimitNOFILE=1048576
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now vmon-netbroker
systemctl enable --now vmon-worker
`;

const workerRole =
  workerArtifactArns.length > 0
    ? new aws.iam.Role("worker-artifacts", {
        assumeRolePolicy: ec2AssumeRolePolicy,
      })
    : undefined;
if (workerRole) {
  new aws.iam.RolePolicy("worker-artifacts", {
    role: workerRole.id,
    policy: artifactReadPolicy(workerArtifactArns),
  });
}
const workerProfile = workerRole
  ? new aws.iam.InstanceProfile("worker-artifacts", { role: workerRole.name })
  : undefined;

const workerLaunchTemplate = new aws.ec2.LaunchTemplate("worker", {
  imageId: ami,
  instanceType: workerInstanceType,
  vpcSecurityGroupIds: [workerSg.id],
  iamInstanceProfile: workerProfile ? { name: workerProfile.name } : undefined,
  userData: workerUserData.apply((data) =>
    Buffer.from(data).toString("base64"),
  ),
  metadataOptions: { httpTokens: "required", httpPutResponseHopLimit: 2 },
  // Virtual instances need the nested-virtualization CPU option for
  // /dev/kvm; metal instances reject it (they have real VT-x/EL2).
  cpuOptions: workerIsMetal ? undefined : { nestedVirtualization: "enabled" },
  blockDeviceMappings: [
    {
      deviceName: "/dev/xvda",
      ebs: { volumeSize: 100, volumeType: "gp3", deleteOnTermination: "true" },
    },
  ],
  tagSpecifications: [
    {
      resourceType: "instance",
      tags: { Name: "vibevmm-worker", "vibevmm:role": "worker" },
    },
  ],
});

const workerAsg = new aws.autoscaling.Group(
  "worker",
  {
    vpcZoneIdentifiers: subnetIds,
    minSize: workerMin,
    maxSize: workerMax,
    desiredCapacity: workerMin,
    launchTemplate: { id: workerLaunchTemplate.id, version: "$Latest" },
    healthCheckType: "EC2",
    // The vmon autoscaler is the only thing allowed to pick victims: it
    // drains a worker first, then terminates it by instance id.
    protectFromScaleIn: true,
    suspendedProcesses: ["AZRebalance"],
    tags: [
      { key: "Name", value: "vibevmm-worker", propagateAtLaunch: true },
      { key: "vibevmm:role", value: "worker", propagateAtLaunch: true },
    ],
    // Desired capacity is runtime state owned by the vmon autoscaler; do not
    // fight it on subsequent `pulumi up`s (ignoreChanges below).
  },
  { ignoreChanges: ["desiredCapacity"] },
);

// ── Scheduler IAM: exactly the two scaling actions, scoped to the ASG ──────
const schedRole = new aws.iam.Role("sched", {
  assumeRolePolicy: ec2AssumeRolePolicy,
});
new aws.iam.RolePolicy("sched-scaling", {
  role: schedRole.id,
  policy: workerAsg.arn.apply((asgArn) =>
    JSON.stringify({
      Version: "2012-10-17",
      Statement: [
        {
          Effect: "Allow",
          Action: [
            "autoscaling:SetDesiredCapacity",
            "autoscaling:TerminateInstanceInAutoScalingGroup",
          ],
          Resource: asgArn,
        },
        {
          Effect: "Allow",
          Action: [
            "autoscaling:DescribeAutoScalingGroups",
            "ec2:DescribeInstances",
          ],
          Resource: "*",
        },
      ],
    }),
  ),
});
if (binaryS3Uri) {
  new aws.iam.RolePolicy("sched-artifacts", {
    role: schedRole.id,
    policy: artifactReadPolicy([s3ObjectArn(binaryS3Uri)]),
  });
}
const schedProfile = new aws.iam.InstanceProfile("sched", {
  role: schedRole.name,
});

// ── Schedulers: vmon sched + scale hooks driving the worker ASG ────────────
const schedUserData = pulumi
  .all([redisUrl, apiToken.result, workerToken.result, workerAsg.name, region])
  .apply(
    ([redis, api, worker, asgName, awsRegion]) => `#!/bin/bash
set -euxo pipefail
${binaryInstall}
chmod +x /usr/local/bin/vmon
mkdir -p /opt/vmon /etc/vmon

cat > /opt/vmon/scale-up.sh <<'EOF'
#!/bin/bash
set -eu
exec aws autoscaling set-desired-capacity \\
  --region ${awsRegion} \\
  --auto-scaling-group-name ${asgName} \\
  --desired-capacity "$VMON_SCALE_DESIRED"
EOF

cat > /opt/vmon/scale-down.sh <<'EOF'
#!/bin/bash
# Terminate only workers the vmon autoscaler reports as drained AND empty;
# still-draining workers are left for a later tick.
set -u
for wid in $VMON_IDLE_WIDS; do
  aws autoscaling terminate-instance-in-auto-scaling-group \\
    --region ${awsRegion} \\
    --instance-id "$wid" \\
    --should-decrement-desired-capacity --no-cli-pager || true
done
EOF
chmod +x /opt/vmon/scale-up.sh /opt/vmon/scale-down.sh

cat > /etc/vmon/sched.env <<'EOF'
VMON_ORCH_REDIS=${redis}
VMON_API_TOKEN=${api}
VMON_WORKER_TOKEN=${worker}
EOF
chmod 600 /etc/vmon/sched.env

cat > /etc/systemd/system/vmon-sched.service <<'EOF'
[Unit]
Description=vibevmm sandbox scheduler
After=network-online.target
Wants=network-online.target

[Service]
EnvironmentFile=/etc/vmon/sched.env
ExecStart=/usr/local/bin/vmon sched --listen 0.0.0.0:${schedPort} \\
  --autoscale-min ${workerMin} --autoscale-max ${workerMax} \\
  --autoscale-target-util ${targetUtil} \\
  --scale-up-cmd /opt/vmon/scale-up.sh \\
  --scale-down-cmd /opt/vmon/scale-down.sh
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now vmon-sched
`,
  );

const schedulerUrls: pulumi.Output<string>[] = [];
for (let index = 0; index < schedulerCount; index += 1) {
  const instance = new aws.ec2.Instance(`sched-${index}`, {
    ami,
    instanceType: schedulerInstanceType,
    subnetId: subnetIds[index % subnetIds.length],
    vpcSecurityGroupIds: [schedSg.id],
    iamInstanceProfile: schedProfile.name,
    userData: schedUserData,
    userDataReplaceOnChange: true,
    metadataOptions: { httpTokens: "required" },
    rootBlockDevice: { volumeSize: 20, volumeType: "gp3" },
    tags: { Name: `vibevmm-sched-${index}`, "vibevmm:role": "scheduler" },
  });
  const eip = new aws.ec2.Eip(`sched-${index}`, {
    instance: instance.id,
    domain: "vpc",
  });
  schedulerUrls.push(pulumi.interpolate`http://${eip.publicIp}:${schedPort}`);
}

// ── Outputs ─────────────────────────────────────────────────────────────────
export const schedulerEndpoints = pulumi.all(schedulerUrls);
/** Scheduler HTTP/gRPC endpoints; `/` serves the fleet dashboard. */
export const schedulerDashboardEndpoints = pulumi.all(schedulerUrls);
export const workerAsgName = workerAsg.name;
export const stateHost = stateInstance.privateIp;
export const apiTokenOut = pulumi.secret(apiToken.result);
export const workerTokenOut = pulumi.secret(workerToken.result);
export const redisUrlOut = pulumi.secret(redisUrl);
export const postgresUrlOut = pulumi.secret(postgresUrl);
