#!/usr/bin/env bash
# Build and atomically roll out vmon to every live GCP worker and scheduler.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
PROJECT=${CLOUDSDK_CORE_PROJECT:-}
TARGET=${VMON_TARGET:-x86_64-unknown-linux-gnu.2.34}
BUILD=1
BINARY=
ROLES=all
GCS_URI=${VMON_BINARY_GCS_URI:-}

usage() {
	cat <<'EOF'
Usage: deploy/gcp/rollout.sh [OPTIONS]

Builds vmon, discovers current instances by their vibevmm-role label, opens
SSH only to this host for the duration of the rollout, and atomically updates
each worker and scheduler. No instance names or IP addresses are stored in the
script.

Options:
  --binary PATH       Deploy an existing binary and skip the build
  --no-build          Use the default target binary without rebuilding
  --project PROJECT   GCP project (defaults to gcloud config)
  --roles ROLES       all, worker, or scheduler (default: all)
  --gcs-uri URI       Also update the MIG launch artifact, e.g. gs://bucket/vmon
  -h, --help          Show this help

Environment:
  VMON_TARGET          cargo-zigbuild target (default: x86_64 Linux glibc 2.34)
  VMON_BINARY_GCS_URI  Same as --gcs-uri
  VMON_SOURCE_CIDR     SSH source CIDR (default: this host's public IPv4 /32)
EOF
}

while (($#)); do
	case "$1" in
		--binary)
			[[ $# -ge 2 ]] || { echo "--binary requires a path" >&2; exit 2; }
			BINARY=$2
			BUILD=0
			shift 2
			;;
		--no-build)
			BUILD=0
			shift
			;;
		--project)
			[[ $# -ge 2 ]] || { echo "--project requires a value" >&2; exit 2; }
			PROJECT=$2
			shift 2
			;;
		--roles)
			[[ $# -ge 2 ]] || { echo "--roles requires a value" >&2; exit 2; }
			ROLES=$2
			shift 2
			;;
		--gcs-uri)
			[[ $# -ge 2 ]] || { echo "--gcs-uri requires a value" >&2; exit 2; }
			GCS_URI=$2
			shift 2
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "unknown option: $1" >&2
			usage >&2
			exit 2
			;;
	esac
done

case "$ROLES" in
	all|worker|scheduler) ;;
	*) echo "--roles must be all, worker, or scheduler" >&2; exit 2 ;;
esac

for command in gcloud curl; do
	command -v "$command" >/dev/null || { echo "required command not found: $command" >&2; exit 1; }
done

if [[ -z "$PROJECT" ]]; then
	PROJECT=$(gcloud config get-value project 2>/dev/null || true)
fi
[[ -n "$PROJECT" && "$PROJECT" != "(unset)" ]] || { echo "set a gcloud project or pass --project" >&2; exit 1; }

if [[ -z "$BINARY" ]]; then
	OUTPUT_TARGET=${TARGET%.2.34}
	BINARY="$ROOT/target/$OUTPUT_TARGET/release/vmon"
fi
if ((BUILD)); then
	command -v cargo-zigbuild >/dev/null || {
		echo "cargo-zigbuild is required; install it or pass --binary" >&2
		exit 1
	}
	(
		cd "$ROOT"
		cargo zigbuild --locked --release --target "$TARGET" -p vmon
	)
fi
[[ -x "$BINARY" ]] || { echo "vmon binary is missing or not executable: $BINARY" >&2; exit 1; }

if [[ -n "$GCS_URI" ]]; then
	echo "Updating launch artifact: $GCS_URI"
	gcloud storage cp "$BINARY" "$GCS_URI" --project "$PROJECT" --no-user-output-enabled
else
	echo "Warning: current instances will be updated, but the MIG launch artifact is unchanged." >&2
	echo "Pass --gcs-uri so replacement workers boot this binary." >&2
fi

TMP=$(mktemp -d "${TMPDIR:-/tmp}/vmon-rollout.XXXXXX")
INVENTORY="$TMP/instances.tsv"
SSH_RULE=""

cleanup() {
	status=$?
	trap - EXIT INT TERM
	if [[ -n "$SSH_RULE" ]]; then
		gcloud compute firewall-rules delete "$SSH_RULE" \
			--project "$PROJECT" --quiet >/dev/null 2>&1 || true
	fi
	rm -rf "$TMP"
	exit "$status"
}
trap cleanup EXIT INT TERM

gcloud compute instances list \
	--project "$PROJECT" \
	--filter='status=RUNNING AND labels.vibevmm-role:(worker scheduler)' \
	--format='value[separator="	"](labels.vibevmm-role,name,zone.basename(),networkInterfaces[0].accessConfigs[0].natIP,networkInterfaces[0].network.basename())' \
	> "$INVENTORY"

[[ -s "$INVENTORY" ]] || { echo "no running vibevmm workers or schedulers found in $PROJECT" >&2; exit 1; }

NETWORK=$(head -n1 "$INVENTORY" | cut -f5)

SOURCE_CIDR=${VMON_SOURCE_CIDR:-}
if [[ -z "$SOURCE_CIDR" ]]; then
	SOURCE_IP=$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')
	[[ "$SOURCE_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
		echo "could not determine this host's public IPv4" >&2
		exit 1
	}
	SOURCE_CIDR="$SOURCE_IP/32"
fi

SSH_RULE="vibevmm-rollout-ssh-$$"
gcloud compute firewall-rules create "$SSH_RULE" \
	--project "$PROJECT" \
	--network "$NETWORK" \
	--direction INGRESS \
	--allow tcp:22 \
	--source-ranges "$SOURCE_CIDR" \
	--target-tags vibevmm-worker,vibevmm-sched \
	--quiet >/dev/null

role_selected() {
	[[ "$ROLES" == all || "$ROLES" == "$1" ]]
}

rollout_instance() {
	local role=$1 name=$2 zone=$3 public_ip=$4
	local remote="/tmp/vmon-rollout-$$"
	local uploaded=0

	[[ -n "$public_ip" ]] || {
		echo "$role $name has no public IPv4; cannot deploy without a bastion" >&2
		return 1
	}
	echo "Deploying $role $name ($public_ip, $zone)"

	for attempt in 1 2 3 4 5; do
		if gcloud compute scp "$BINARY" "$name:$remote" \
			--project "$PROJECT" --zone "$zone" --quiet; then
			uploaded=1
			break
		fi
		sleep 2
	done
	((uploaded)) || { echo "upload failed for $name" >&2; return 1; }

	gcloud compute ssh "$name" \
		--project "$PROJECT" --zone "$zone" --quiet \
		--command "sudo bash -s -- '$role' '$remote'" <<'REMOTE'
set -euo pipefail
role=$1
uploaded=$2
next=/usr/local/bin/vmon.next
previous=/usr/local/bin/vmon.previous
case "$role" in
	worker)
		services=(vmon-netbroker vmon-worker)
		port=8000
		;;
	scheduler)
		services=(vmon-sched)
		port=8100
		;;
	*)
		echo "unsupported role: $role" >&2
		exit 2
		;;
esac

install -o root -g root -m 0755 "$uploaded" "$next"
cp -p /usr/local/bin/vmon "$previous"
mv -f "$next" /usr/local/bin/vmon
rollback() {
	trap - ERR
	if [[ -f "$previous" ]]; then
		mv -f "$previous" /usr/local/bin/vmon
		systemctl restart "${services[@]}" || true
	fi
}
trap rollback ERR
systemctl restart "${services[@]}"
for service in "${services[@]}"; do
	systemctl is-active --quiet "$service"
done
for _ in {1..30}; do
	if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null; then
		rm -f "$previous" "$uploaded"
		trap - ERR
		/usr/local/bin/vmon --version
		exit 0
	fi
	sleep 1
done
echo "$role health check timed out on port $port" >&2
exit 1
REMOTE
}

DEPLOYED=0
for wanted_role in worker scheduler; do
	role_selected "$wanted_role" || continue
	while IFS=$'\t' read -r role name zone public_ip _network; do
		[[ "$role" == "$wanted_role" ]] || continue
		rollout_instance "$role" "$name" "$zone" "$public_ip"
		DEPLOYED=$((DEPLOYED + 1))
	done < "$INVENTORY"
done

((DEPLOYED > 0)) || { echo "no running instances matched --roles $ROLES" >&2; exit 1; }

echo "Rollout complete: $DEPLOYED instance(s)"
while IFS=$'\t' read -r role _name _zone public_ip _network; do
	if [[ "$role" == scheduler ]]; then
		echo "Scheduler: http://$public_ip:8100"
	fi
done < "$INVENTORY"
