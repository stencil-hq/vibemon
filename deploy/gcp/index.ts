/**
 * vibevmm orchestration layer on GCP.
 *
 * Topology (N×M, brokerless — mirrors vmond/src/orch and deploy/aws):
 *
 *   clients ──▶ scheduler VM (vmon sched, static IP each) ──direct gRPC──▶ worker MIG (vmon serve)
 *                     │  in-memory worker table                                   │ self-published heartbeats
 *                     └────────── follows stream ──────────▶ state VM ◀───────────┘
 *                                                     (Redis 6379 + Postgres 5432)
 *
 * Cost stance (deliberate): no Memorystore, no Cloud SQL, no Cloud NAT, no LB
 * by default. One small VM runs both Redis (orch state bus) and Postgres
 * (vmond cluster substrate) — the orch layer treats Redis as reconstructible
 * cache, so a single box is an accepted trade, not an oversight.
 *
 * KVM: unlike EC2, GCE exposes nested virtualization as a plain instance
 * template flag on Intel machine series (N1/N2/C2/C3, …) — no bare metal
 * required for x86_64 workers. E2 and AMD series do not support it. Arm
 * (T2A/C4A) has no nested virtualization either, so arm64 fleets require an
 * explicit `-metal` machine type.
 *
 * Autoscaling: the vmon sched leader computes desired capacity (HPA-like) and
 * drives THIS stack's worker MIG through the scale hooks:
 *   scale-up.sh   → gcloud compute instance-groups managed resize --size $VMON_SCALE_DESIRED
 *   scale-down.sh → gcloud compute instance-groups managed delete-instances
 *                   for each $VMON_IDLE_WIDS entry (drained AND empty workers only)
 * Worker ids ARE instance names (VMON_ORCH_ID = metadata instance/name), which
 * makes the delete mapping trivial; delete-instances decrements the target
 * size atomically. No GCP autoscaler is attached and instance redistribution
 * is disabled, so GCP never picks victims itself.
 */

import * as gcp from "@pulumi/gcp";
import * as pulumi from "@pulumi/pulumi";
import * as random from "@pulumi/random";

const config = new pulumi.Config();
const gcpConfig = new pulumi.Config("gcp");

/** GCS URI preferred for IAM-authenticated binary downloads by autoscaled instances. */
const binaryGcsUri = config.get("binaryGcsUri");
/** HTTPS fallback for deployments that host the binary outside GCS. */
const binaryUrl = binaryGcsUri ? undefined : config.require("binaryUrl");
/** Optional GCS tarball with kernel/agent assets. */
const assetsGcsUri = config.get("assetsGcsUri");
/** HTTPS fallback for deployments that host assets outside GCS. */
const assetsUrl = assetsGcsUri ? undefined : config.get("assetsUrl");
/** CIDR allowed to reach schedulers (and worker endpoints for direct dials). */
const allowedCidr = config.get("allowedCidr") ?? "0.0.0.0/0";
/** Worker fleet bounds; the vmon autoscaler moves the MIG target size in [min, max]. */
const workerMin = config.getNumber("workerMin") ?? 1;
const workerMax = config.getNumber("workerMax") ?? 4;
/** Guest architecture of the fleet; must match the vmon binary at binaryUrl. */
const arch = config.get("arch") ?? "x86_64";
/** Intel series get /dev/kvm via the nested-virtualization flag; Arm needs metal. */
const workerMachineType =
  config.get("workerMachineType") ??
  (arch === "arm64" ? undefined : "n2-standard-8");
if (!workerMachineType) {
  throw new Error(
    "arm64 workers need an explicit workerMachineType: GCE has no Arm nested virtualization, so only `-metal` machine types expose /dev/kvm",
  );
}
const workerIsMetal = workerMachineType.endsWith("-metal");
if (arch === "arm64" && !workerIsMetal) {
  throw new Error(
    `arm64 worker machine type must be bare metal (got ${workerMachineType}): Arm series have no nested virtualization`,
  );
}
const schedulerMachineType =
  config.get("schedulerMachineType") ??
  (arch === "arm64" ? "t2a-standard-1" : "e2-small");
const stateMachineType =
  config.get("stateMachineType") ??
  (arch === "arm64" ? "t2a-standard-1" : "e2-small");
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

const region = gcpConfig.require("region");
const project = gcp.organizations.getClientConfigOutput().project;
const zones = gcp.compute.getZonesOutput({ region, status: "UP" });
const zonePair = zones.names.apply((names) => names.slice(0, 2));
const image = gcp.compute.getImageOutput({
  family: arch === "arm64" ? "debian-12-arm64" : "debian-12",
  project: "debian-cloud",
}).selfLink;

function parseGcsUri(uri: string): { bucket: string; object: string } {
  const match = /^gs:\/\/([^/]+)\/(.+)$/.exec(uri);
  if (!match) {
    throw new Error(`invalid GCS artifact URI: ${uri}`);
  }
  return { bucket: match[1], object: match[2] };
}

/**
 * Shell snippet fetching a GCS object with the instance service-account token
 * from the metadata server — no gcloud needed on workers.
 */
function gcsFetch(uri: string): string {
  const { bucket, object } = parseGcsUri(uri);
  return (
    `curl -fsSL -H "Authorization: Bearer $(curl -s -H 'Metadata-Flavor: Google' ` +
    `http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token ` +
    `| sed -E 's/.*"access_token" *: *"([^"]+)".*/\\1/')" ` +
    `"https://storage.googleapis.com/storage/v1/b/${bucket}/o/${encodeURIComponent(object)}?alt=media"`
  );
}

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

// ── Network: custom-mode VPC, one regional subnet (no Cloud NAT $$$) ───────
const network = new gcp.compute.Network("orch", {
  name: "vibevmm-orch",
  autoCreateSubnetworks: false,
});
const subnet = new gcp.compute.Subnetwork("orch", {
  name: "vibevmm-orch",
  network: network.id,
  region,
  ipCidrRange: "10.42.0.0/16",
});

// ── Firewall: network tags play the role of AWS security groups ────────────
new gcp.compute.Firewall("sched-ingress", {
  name: "vibevmm-sched-ingress",
  network: network.id,
  direction: "INGRESS",
  sourceRanges: [allowedCidr],
  targetTags: ["vibevmm-sched"],
  allows: [
    { protocol: "tcp", ports: [`${schedPort}`, `${dashboardPort}`] },
  ],
});
new gcp.compute.Firewall("worker-direct", {
  name: "vibevmm-worker-direct",
  network: network.id,
  direction: "INGRESS",
  sourceRanges: [allowedCidr],
  targetTags: ["vibevmm-worker"],
  allows: [{ protocol: "tcp", ports: [`${workerPort}`] }],
});
new gcp.compute.Firewall("worker-from-sched", {
  name: "vibevmm-worker-from-sched",
  network: network.id,
  direction: "INGRESS",
  sourceTags: ["vibevmm-sched"],
  targetTags: ["vibevmm-worker"],
  allows: [{ protocol: "tcp", ports: [`${workerPort}`] }],
});
new gcp.compute.Firewall("state-ingress", {
  name: "vibevmm-state-ingress",
  network: network.id,
  direction: "INGRESS",
  sourceTags: ["vibevmm-sched", "vibevmm-worker"],
  targetTags: ["vibevmm-state"],
  allows: [{ protocol: "tcp", ports: ["6379", "5432"] }],
});

// ── Service accounts: dedicated, roleless by default ───────────────────────
// A dedicated empty SA beats the project-default compute SA (broad legacy
// grants); roles are attached below only where needed.
const workerSa = new gcp.serviceaccount.Account("worker", {
  accountId: "vibevmm-worker",
  displayName: "vibevmm worker",
});
const schedSa = new gcp.serviceaccount.Account("sched", {
  accountId: "vibevmm-sched",
  displayName: "vibevmm scheduler",
});
const stateSa = new gcp.serviceaccount.Account("state", {
  accountId: "vibevmm-state",
  displayName: "vibevmm state VM",
});

const artifactBuckets = new Set<string>();
if (binaryGcsUri) {
  artifactBuckets.add(parseGcsUri(binaryGcsUri).bucket);
}
if (assetsGcsUri) {
  artifactBuckets.add(parseGcsUri(assetsGcsUri).bucket);
}
for (const bucket of artifactBuckets) {
  new gcp.storage.BucketIAMMember(`worker-artifacts-${bucket}`, {
    bucket,
    role: "roles/storage.objectViewer",
    member: pulumi.interpolate`serviceAccount:${workerSa.email}`,
  });
}
if (binaryGcsUri) {
  new gcp.storage.BucketIAMMember("sched-artifacts", {
    bucket: parseGcsUri(binaryGcsUri).bucket,
    role: "roles/storage.objectViewer",
    member: pulumi.interpolate`serviceAccount:${schedSa.email}`,
  });
}

// ── State VM: Redis + Postgres ─────────────────────────────────────────────
// GCE startup scripts rerun on every boot; the marker keeps them once-only
// like EC2 user data.
const stateStartup = pulumi.interpolate`#!/bin/bash
set -euxo pipefail
[ -f /var/lib/vibevmm-init-done ] && exit 0
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y redis-server postgresql

# redis: bind to the VPC, password auth
sed -i 's/^bind .*/bind 0.0.0.0 -::1/' /etc/redis/redis.conf
sed -i 's/^protected-mode yes/protected-mode no/' /etc/redis/redis.conf
echo 'requirepass ${redisPassword.result}' >> /etc/redis/redis.conf
systemctl enable redis-server
systemctl restart redis-server

# postgres: VPC-local scram auth for the vmon role
PG_CONF_DIR=$(ls -d /etc/postgresql/*/main)
echo "listen_addresses = '*'" >> "$PG_CONF_DIR/postgresql.conf"
echo "host all vmon 10.42.0.0/16 scram-sha-256" >> "$PG_CONF_DIR/pg_hba.conf"
systemctl enable postgresql
systemctl restart postgresql
sudo -u postgres psql -c "CREATE ROLE vmon LOGIN PASSWORD '${pgPassword.result}';"
sudo -u postgres createdb -O vmon vmon

touch /var/lib/vibevmm-init-done
`;
const stateInstance = new gcp.compute.Instance("state", {
  name: "vibevmm-state",
  zone: zones.names.apply((names) => names[0]),
  machineType: stateMachineType,
  bootDisk: {
    initializeParams: { image, size: 20, type: "pd-balanced" },
  },
  networkInterfaces: [
    {
      network: network.id,
      subnetwork: subnet.id,
      accessConfigs: [{}], // ephemeral public IP for apt; ingress is tag-gated
    },
  ],
  tags: ["vibevmm-state"],
  labels: { "vibevmm-role": "state" },
  metadataStartupScript: stateStartup,
  serviceAccount: { email: stateSa.email, scopes: ["cloud-platform"] },
});
const stateHostIp = stateInstance.networkInterfaces.apply(
  (interfaces) => interfaces[0].networkIp,
);

const redisUrl = pulumi.interpolate`redis://:${redisPassword.result}@${stateHostIp}:6379`;
const postgresUrl = pulumi.interpolate`postgres://vmon:${pgPassword.result}@${stateHostIp}:5432/vmon`;

// ── Workers: nested-virt MIG, vmon-driven sizing ────────────────────────────
const binaryInstall = binaryGcsUri
  ? `${gcsFetch(binaryGcsUri)} -o /usr/local/bin/vmon`
  : `curl -fsSL "${binaryUrl}" -o /usr/local/bin/vmon`;
const assetsSnippet = assetsGcsUri
  ? `mkdir -p /var/lib/vmon/assets
${gcsFetch(assetsGcsUri)} | tar -xz -C /var/lib/vmon/assets`
  : assetsUrl
    ? `mkdir -p /var/lib/vmon/assets
curl -fsSL "${assetsUrl}" | tar -xz -C /var/lib/vmon/assets`
    : "true # no assets tarball configured";
const ociArch = arch === "arm64" ? "arm64" : "amd64";

const workerStartup = pulumi.interpolate`#!/bin/bash
set -euxo pipefail
[ -f /var/lib/vibevmm-init-done ] && exit 0
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y curl iptables

getent group vmon >/dev/null || groupadd --system vmon
if ! id -u vmon >/dev/null 2>&1; then
  useradd --system --gid vmon --home-dir /var/lib/vmon --shell /usr/sbin/nologin --comment "Vibemon worker" vmon
fi
if getent group kvm >/dev/null; then
  usermod --append --groups kvm vmon
fi
install -d -o vmon -g vmon -m 0700 /var/lib/vmon
install -d -o root -g vmon -m 0750 /etc/vmon

${binaryInstall}
chmod 0755 /usr/local/bin/vmon
${assetsSnippet}

# OCI image pipeline tools (Debian's are dynamic; static release builds).
# skopeo has no official static artifact — lework/skopeo-binary is the
# community-maintained build; pin versions and swap for your own mirror in
# security-sensitive deployments.
curl -fsSL https://github.com/lework/skopeo-binary/releases/download/v1.16.1/skopeo-linux-${ociArch} -o /usr/local/bin/skopeo
curl -fsSL https://github.com/opencontainers/umoci/releases/download/v0.4.7/umoci.${ociArch} -o /usr/local/bin/umoci
chmod 0755 /usr/local/bin/skopeo /usr/local/bin/umoci
mkdir -p /etc/containers
printf 'unqualified-search-registries = ["docker.io"]\\nshort-name-mode = "permissive"\\n' > /etc/containers/registries.conf
cat > /etc/containers/policy.json <<'EOF'
{
  "default": [{ "type": "insecureAcceptAnything" }]
}
EOF

INSTANCE_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
PRIVATE_IP=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)

cat > /etc/vmon/worker.env <<EOF
VMON_HOME=/var/lib/vmon
VMON_API_TOKEN=${workerToken.result}
VMON_ORCH_REDIS=${redisUrl}
VMON_ORCH_ID=$INSTANCE_NAME
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

touch /var/lib/vibevmm-init-done
`;

const workerTemplate = new gcp.compute.InstanceTemplate("worker", {
  namePrefix: "vibevmm-worker-",
  machineType: workerMachineType,
  disks: [
    {
      sourceImage: image,
      autoDelete: true,
      boot: true,
      diskSizeGb: 100,
      diskType: "pd-balanced",
    },
  ],
  networkInterfaces: [
    {
      network: network.id,
      subnetwork: subnet.id,
      accessConfigs: [{}], // public IP: artifact pulls + direct endpoint dials, no NAT
    },
  ],
  tags: ["vibevmm-worker"],
  labels: { "vibevmm-role": "worker" },
  metadataStartupScript: workerStartup,
  serviceAccount: { email: workerSa.email, scopes: ["cloud-platform"] },
  // Virtual instances need the nested-virtualization flag for /dev/kvm
  // (Intel series only); metal machine types have real VT-x/EL2.
  advancedMachineFeatures: workerIsMetal
    ? undefined
    : { enableNestedVirtualization: true },
});

const workerMig = new gcp.compute.RegionInstanceGroupManager(
  "worker",
  {
    name: "vibevmm-worker",
    region,
    baseInstanceName: "vibevmm-worker",
    versions: [{ instanceTemplate: workerTemplate.id }],
    targetSize: workerMin,
    distributionPolicyZones: zonePair,
    // The vmon autoscaler is the only thing allowed to pick victims: it
    // drains a worker first, then deletes it by instance name. OPPORTUNISTIC
    // + redistribution NONE means GCP never replaces or moves instances on
    // its own (the AWS analog of scale-in protection + suspended AZRebalance).
    updatePolicy: {
      type: "OPPORTUNISTIC",
      minimalAction: "REPLACE",
      instanceRedistributionType: "NONE",
      maxSurgeFixed: 0,
      maxUnavailableFixed: 2,
    },
    // Target size is runtime state owned by the vmon autoscaler; do not
    // fight it on subsequent `pulumi up`s (ignoreChanges below).
  },
  { ignoreChanges: ["targetSize"] },
);

// ── Scheduler IAM: exactly the resize/delete permissions, custom role ──────
const schedScalingRole = new gcp.projects.IAMCustomRole("sched-scaling", {
  roleId: "vibevmmSchedScaling",
  title: "vibevmm scheduler scaling",
  description: "resize the vibevmm worker MIG and delete drained workers",
  permissions: [
    "compute.instanceGroupManagers.get",
    "compute.instanceGroupManagers.update",
    "compute.instances.delete",
    "compute.instances.get",
    "compute.instances.list",
  ],
});
new gcp.projects.IAMMember("sched-scaling", {
  project,
  role: schedScalingRole.name,
  member: pulumi.interpolate`serviceAccount:${schedSa.email}`,
});

// ── Schedulers: vmon sched + scale hooks driving the worker MIG ────────────
const schedStartup = pulumi
  .all([redisUrl, apiToken.result, workerToken.result, workerMig.name])
  .apply(
    ([redis, api, worker, migName]) => `#!/bin/bash
set -euxo pipefail
[ -f /var/lib/vibevmm-init-done ] && exit 0
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y curl ca-certificates gnupg apt-transport-https

# gcloud for the scale hooks; auth rides the instance service account.
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list
apt-get update
apt-get install -y google-cloud-cli

${binaryInstall}
chmod +x /usr/local/bin/vmon
mkdir -p /opt/vmon /etc/vmon

cat > /opt/vmon/scale-up.sh <<'EOF'
#!/bin/bash
set -eu
exec gcloud compute instance-groups managed resize ${migName} \\
  --region ${region} --size "$VMON_SCALE_DESIRED" --quiet
EOF

cat > /opt/vmon/scale-down.sh <<'EOF'
#!/bin/bash
# Delete only workers the vmon autoscaler reports as drained AND empty;
# still-draining workers are left for a later tick. delete-instances
# decrements the MIG target size atomically.
set -u
for wid in $VMON_IDLE_WIDS; do
  gcloud compute instance-groups managed delete-instances ${migName} \\
    --region ${region} --instances "$wid" --quiet || true
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

touch /var/lib/vibevmm-init-done
`,
  );

const schedulerUrls: pulumi.Output<string>[] = [];
for (let index = 0; index < schedulerCount; index += 1) {
  const address = new gcp.compute.Address(`sched-${index}`, {
    name: `vibevmm-sched-${index}`,
    region,
  });
  new gcp.compute.Instance(`sched-${index}`, {
    name: `vibevmm-sched-${index}`,
    zone: zonePair.apply((pair) => pair[index % pair.length]),
    machineType: schedulerMachineType,
    bootDisk: {
      initializeParams: { image, size: 20, type: "pd-balanced" },
    },
    networkInterfaces: [
      {
        network: network.id,
        subnetwork: subnet.id,
        accessConfigs: [{ natIp: address.address }],
      },
    ],
    tags: ["vibevmm-sched"],
    labels: { "vibevmm-role": "scheduler" },
    metadataStartupScript: schedStartup,
    serviceAccount: { email: schedSa.email, scopes: ["cloud-platform"] },
  });
  schedulerUrls.push(pulumi.interpolate`http://${address.address}:${schedPort}`);
}

// ── Outputs ─────────────────────────────────────────────────────────────────
export const schedulerEndpoints = pulumi.all(schedulerUrls);
/** Scheduler HTTP/gRPC endpoints; `/` serves the fleet dashboard. */
export const schedulerDashboardEndpoints = pulumi.all(schedulerUrls);
export const workerMigName = workerMig.name;
export const stateHost = stateHostIp;
export const apiTokenOut = pulumi.secret(apiToken.result);
export const workerTokenOut = pulumi.secret(workerToken.result);
export const redisUrlOut = pulumi.secret(redisUrl);
export const postgresUrlOut = pulumi.secret(postgresUrl);
