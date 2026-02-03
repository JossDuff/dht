#!/bin/bash

# sun - Run Rust projects across Sunlab cluster nodes

set -e

# Configuration
USERNAME="jod323"
DOMAIN="cse.lehigh.edu"
LOG_DIR="logs"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"

# All known Sunlab nodes
ALL_NODES=(
    ariel caliban callisto ceres
    chiron cupid eris europa hydra
    iapetus io ixion mars mercury
    neptune nereid nix orcus phobos puck
    saturn triton varda vesta xena
)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

usage() {
    cat <<'EOF'
Usage: sun <command> [options]

Commands:
  list                              List all nodes with their current CPU load
  run -n <num> [-- args...]         Run 'target/release/dht' on N least-loaded nodes
  exec -n <num> -- <command>        Run arbitrary command on N least-loaded nodes
  kill [name]                       Kill processes on all nodes (default: 'dht')

Options:
  -n <num>      Number of nodes to use (required for 'run' and 'exec')
  -k <num>      Number of keys for benchmark (passed to dht)
  -d <dir>      Project directory (default: current directory)
  -h, --help    Show this help

Examples:
  sun list
  sun run -n 3
  sun run -n 3 -k 100000
  sun run -n 3 -- --keys 1000 --ops 50000
  sun run -n 5 -d ~/dev/cse476/project -- --config config.toml
  sun exec -n 3 -- hostname
  sun exec -n 5 -- "cd ~/dev/project && ./my_script.sh"
  sun kill                          Kill 'dht' on all nodes
  sun kill myapp                    Kill 'myapp' on all nodes

Logs are saved to: logs/<node>.log

Ctrl+C stops all nodes and cleans up.
EOF
    exit 0
}

# Get load for a single node
probe_node() {
    local node=$1
    local host="${node}.${DOMAIN}"

    local load=$(ssh -o StrictHostKeyChecking=no \
        -o ConnectTimeout=3 \
        -o BatchMode=yes \
        "${USERNAME}@${host}" \
        "cat /proc/loadavg | cut -d' ' -f1" 2>/dev/null)

    if [[ -n "$load" ]]; then
        echo "$load $node"
    fi
}

# Probe all nodes in parallel and return sorted by load
get_sorted_nodes() {
    echo -e "${CYAN}Probing ${#ALL_NODES[@]} nodes for CPU load...${NC}" >&2

    local tmp_file=$(mktemp)

    for node in "${ALL_NODES[@]}"; do
        probe_node "$node" >>"$tmp_file" &
    done
    wait

    sort -n "$tmp_file"
    rm -f "$tmp_file"
}

# Kill my dht binary running on all machines
cmd_kill() {
    local binary_name="${1:-dht}"

    echo -e "${GREEN}=== Killing '$binary_name' on all nodes ===${NC}"
    echo ""

    for node in "${ALL_NODES[@]}"; do
        local host="${node}.${DOMAIN}"
        result=$(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o BatchMode=yes \
            "${USERNAME}@${host}" "pkill -u $USERNAME $binary_name && echo 'killed' || echo 'none'" 2>/dev/null)
        if [[ "$result" == "killed" ]]; then
            echo -e "${YELLOW}[$node]${NC} killed $binary_name"
        fi
    done &
    wait

    echo ""
    echo -e "${GREEN}Done${NC}"
}

# List command
cmd_list() {
    echo -e "${GREEN}=== Sunlab Node Status ===${NC}"
    echo ""
    printf "%-12s %s\n" "NODE" "LOAD (1min)"
    printf "%-12s %s\n" "----" "----------"

    get_sorted_nodes | while read load node; do
        if (($(echo "$load < 1.0" | bc -l))); then
            color=$GREEN
        elif (($(echo "$load < 3.0" | bc -l))); then
            color=$YELLOW
        else
            color=$RED
        fi
        printf "${color}%-12s %s${NC}\n" "$node" "$load"
    done
    echo ""
}

# Select N least-loaded nodes
select_nodes() {
    local num_nodes=$1

    mapfile -t SORTED < <(get_sorted_nodes)

    if [[ ${#SORTED[@]} -lt $num_nodes ]]; then
        echo -e "${RED}Error: Only ${#SORTED[@]} nodes available, but $num_nodes requested${NC}" >&2
        exit 1
    fi

    echo -e "${GREEN}Selected nodes (by lowest load):${NC}" >&2
    printf "  %-12s %s\n" "NODE" "LOAD" >&2

    for i in $(seq 0 $((num_nodes - 1))); do
        load=$(echo "${SORTED[$i]}" | cut -d' ' -f1)
        node=$(echo "${SORTED[$i]}" | cut -d' ' -f2)
        printf "  ${CYAN}%-12s %s${NC}\n" "$node" "$load" >&2
        echo "$node"
    done
}

# Build --connections arg for a node (all other nodes, comma-separated)
get_connections() {
    local current_node=$1
    shift
    local all_nodes=("$@")

    local connections=""
    for node in "${all_nodes[@]}"; do
        if [[ "$node" != "$current_node" ]]; then
            if [[ -n "$connections" ]]; then
                connections="${connections},${node}"
            else
                connections="$node"
            fi
        fi
    done
    echo "$connections"
}

# Run commands on selected nodes
run_on_nodes() {
    local command="$1"
    shift
    local nodes=("$@")

    rm -rf "$LOG_DIR"
    mkdir -p "$LOG_DIR"

    # Save run metadata
    cat >"$LOG_DIR/run_info.txt" <<EOF
Run started: $(date)
Command: $command
Nodes: ${nodes[*]}
EOF

    echo ""
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
    echo ""

    # Store PIDs for cleanup
    declare -A PIDS

    cleanup() {
        echo ""
        echo -e "${RED}Caught interrupt, stopping all nodes...${NC}"

        # Kill local SSH processes
        for node in "${!PIDS[@]}"; do
            kill "${PIDS[$node]}" 2>/dev/null && echo -e "${YELLOW}[$node]${NC} stopped"
        done

        # Kill remote processes
        for node in "${nodes[@]}"; do
            local host="${node}.${DOMAIN}"
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes \
                "${USERNAME}@${host}" "pkill -u $USERNAME dht" 2>/dev/null
        done

        echo ""
        echo -e "${GREEN}=== Cleanup Complete ===${NC}"
        echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
        exit 0
    }

    trap cleanup SIGINT SIGTERM

    # Start all nodes in background
    for node in "${nodes[@]}"; do
        local host="${node}.${DOMAIN}"
        local log_file="$LOG_DIR/${node}.log"

        echo -e "${YELLOW}[$node]${NC} starting..."

        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            "${USERNAME}@${host}" "$command" \
            >"$log_file" 2>&1 &

        PIDS[$node]=$!
    done

    echo ""
    echo -e "${GREEN}All nodes started. Waiting for completion...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop all nodes${NC}"
    echo ""

    # Wait for all processes
    failed=0
    for node in "${!PIDS[@]}"; do
        if wait "${PIDS[$node]}"; then
            echo -e "${GREEN}[$node]${NC} completed"
        else
            echo -e "${RED}[$node]${NC} failed"
            ((failed++))
        fi
    done

    echo ""
    echo -e "${GREEN}=== Run Complete ===${NC}"
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"

    if [[ $failed -gt 0 ]]; then
        echo -e "${RED}$failed node(s) failed${NC}"
    fi
}

# Run command
cmd_run() {
    local num_nodes="$1"
    local project_dir="$2"
    local num_keys="$3"
    shift 3
    local program_args="$*"

    echo -e "${GREEN}=== Running on Cluster ===${NC}"
    echo ""

    mapfile -t nodes < <(select_nodes "$num_nodes")

    echo ""
    echo -e "Directory: ${BLUE}$project_dir${NC}"
    if [[ -n "$num_keys" ]]; then
        echo -e "Num keys: ${BLUE}$num_keys${NC}"
    fi
    if [[ -n "$program_args" ]]; then
        echo -e "Extra args: ${BLUE}$program_args${NC}"
    fi

    rm -rf "$LOG_DIR"
    mkdir -p "$LOG_DIR"

    echo ""
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
    echo ""

    declare -A PIDS

    cleanup() {
        echo ""
        echo -e "${RED}Caught interrupt, stopping all nodes...${NC}"

        for node in "${!PIDS[@]}"; do
            kill "${PIDS[$node]}" 2>/dev/null && echo -e "${YELLOW}[$node]${NC} stopped"
        done

        for node in "${nodes[@]}"; do
            local host="${node}.${DOMAIN}"
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes \
                "${USERNAME}@${host}" "pkill -u $USERNAME dht" 2>/dev/null
        done

        echo ""
        echo -e "${GREEN}=== Cleanup Complete ===${NC}"
        echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
        exit 0
    }

    trap cleanup SIGINT SIGTERM

    for node in "${nodes[@]}"; do
        local host="${node}.${DOMAIN}"
        local log_file="$LOG_DIR/${node}.log"
        local connections
        connections=$(get_connections "$node" "${nodes[@]}")

        local cmd="$TARGET_DIR/release/dht --name $node --connections $connections"
        if [[ -n "$num_keys" ]]; then
            cmd="$cmd --num-keys $num_keys"
        fi
        if [[ -n "$program_args" ]]; then
            cmd="$cmd $program_args"
        fi

        echo -e "${YELLOW}[$node]${NC} starting (connects to: $connections)"

        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            "${USERNAME}@${host}" "$cmd" \
            >"$log_file" 2>&1 &

        PIDS[$node]=$!
    done

    echo ""
    echo -e "${GREEN}All nodes started. Waiting for completion...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop all nodes${NC}"
    echo ""

    failed=0
    for node in "${!PIDS[@]}"; do
        if wait "${PIDS[$node]}"; then
            echo -e "${GREEN}[$node]${NC} completed"
        else
            echo -e "${RED}[$node]${NC} failed"
            ((failed++))
        fi
    done

    echo ""
    echo -e "${GREEN}=== Run Complete ===${NC}"
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"

    if [[ $failed -gt 0 ]]; then
        echo -e "${RED}$failed node(s) failed${NC}"
    fi
}

# Exec command
cmd_exec() {
    local num_nodes="$1"
    shift
    local command="$*"

    if [[ -z "$command" ]]; then
        echo -e "${RED}Error: No command specified after --${NC}"
        exit 1
    fi

    echo -e "${GREEN}=== Executing on Cluster ===${NC}"
    echo ""

    mapfile -t nodes < <(select_nodes "$num_nodes")

    echo ""
    echo -e "Command: ${BLUE}$command${NC}"

    run_on_nodes "$command" "${nodes[@]}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if [[ $# -eq 0 ]]; then
    usage
fi

COMMAND="$1"
shift

NUM_NODES=""
PROJECT_DIR="$(pwd)"
NUM_KEYS=""
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
    -n)
        NUM_NODES="$2"
        shift 2
        ;;
    -k)
        NUM_KEYS="$2"
        shift 2
        ;;
    -d)
        PROJECT_DIR="$2"
        shift 2
        ;;
    -h | --help)
        usage
        ;;
    --)
        shift
        EXTRA_ARGS="$*"
        break
        ;;
    *)
        echo -e "${RED}Unknown option: $1${NC}"
        usage
        ;;
    esac
done

case "$COMMAND" in
list)
    cmd_list
    ;;
run)
    if [[ -z "$NUM_NODES" ]]; then
        echo -e "${RED}Error: -n <num_nodes> is required for 'run'${NC}"
        echo ""
        usage
    fi
    if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [[ "$NUM_NODES" -lt 1 ]]; then
        echo -e "${RED}Error: Number of nodes must be a positive integer${NC}"
        exit 1
    fi
    cmd_run "$NUM_NODES" "$PROJECT_DIR" "$NUM_KEYS" $EXTRA_ARGS
    ;;
exec)
    if [[ -z "$NUM_NODES" ]]; then
        echo -e "${RED}Error: -n <num_nodes> is required for 'exec'${NC}"
        echo ""
        usage
    fi
    if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [[ "$NUM_NODES" -lt 1 ]]; then
        echo -e "${RED}Error: Number of nodes must be a positive integer${NC}"
        exit 1
    fi
    if [[ -z "$EXTRA_ARGS" ]]; then
        echo -e "${RED}Error: 'exec' requires a command after --${NC}"
        echo ""
        usage
    fi
    cmd_exec "$NUM_NODES" $EXTRA_ARGS
    ;;
kill)
    cmd_kill "${EXTRA_ARGS:-dht}"
    ;;
-h | --help)
    usage
    ;;
*)
    echo -e "${RED}Unknown command: $COMMAND${NC}"
    echo ""
    usage
    ;;
esac
