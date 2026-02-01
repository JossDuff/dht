#!/bin/bash

# sun - Run Rust projects across Sunlab cluster nodes

set -e

# Configuration
USERNAME="jod323"
DOMAIN="cse.lehigh.edu"
SESSION_NAME="sun"
LOG_DIR="logs"

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
  run -n <num> [-- args...]         Run 'cargo run --release' on N least-loaded nodes
  exec -n <num> -- <command>        Run arbitrary command on N least-loaded nodes

Options:
  -n <num>      Number of nodes to use (required for 'run' and 'exec')
  -d <dir>      Project directory (default: current directory)
  -h, --help    Show this help

Examples:
  sun list
  sun run -n 3
  sun run -n 3 -- --keys 1000 --ops 50000
  sun run -n 5 -d ~/dev/cse476/project -- --config config.toml
  sun exec -n 3 -- hostname
  sun exec -n 5 -- "cd ~/dev/project && ./my_script.sh"

Logs are saved to: logs/<node>.log

Ctrl+C stops all nodes and cleans up.

To view live output, attach to the screen session:
  screen -r <session>    Attach to session
  Ctrl-a d               Detach (return to main terminal)
  Ctrl-a "               List all windows (shows node names)
  Ctrl-a n/p             Next/previous window
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

# Run screen session with commands on selected nodes
run_on_nodes() {
    local command="$1"
    shift
    local nodes=("$@")

    rm -rf "$LOG_DIR"
    mkdir -p "$LOG_DIR"

    # Save run metadata
    cat >"$LOG_DIR/run_info.txt" <<EOF
Run started: $(date)
Session: $SESSION_NAME
Command: $command
Nodes: ${nodes[*]}
EOF

    if ! command -v screen &>/dev/null; then
        echo -e "${RED}Error: screen is not installed${NC}"
        exit 1
    fi

    screen -S "$SESSION_NAME" -X quit 2>/dev/null || true

    echo ""
    echo -e "Logs: ${BLUE}$LOG_DIR${NC}"
    echo -e "Session: ${BLUE}$SESSION_NAME${NC}"
    echo ""
    echo -e "${GREEN}Creating screen session...${NC}"

    # Create initial screen session with first node
    first_node="${nodes[0]}"
    first_host="${first_node}.${DOMAIN}"
    first_log="$LOG_DIR/${first_node}.log"
    screen -dmS "$SESSION_NAME" -t "$first_node" bash -c "
        echo '════════════════════════════════════════════════════════════════'
        echo '  NODE: $first_node'
        echo '  HOST: $first_host'
        echo '  TIME: '\$(date)
        echo '════════════════════════════════════════════════════════════════'
        echo ''
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 ${USERNAME}@${first_host} '$command' 2>&1 | tee '$first_log'
        echo ''
        echo '════════════════════════════════════════════════════════════════'
        echo '  Done. Log: $first_log'
        echo '════════════════════════════════════════════════════════════════'
    "

    # Add windows for remaining nodes
    for i in $(seq 1 $((${#nodes[@]} - 1))); do
        node="${nodes[$i]}"
        host="${node}.${DOMAIN}"
        log_file="$LOG_DIR/${node}.log"
        screen -S "$SESSION_NAME" -X screen -t "$node" bash -c "
            echo '════════════════════════════════════════════════════════════════'
            echo '  NODE: $node'
            echo '  HOST: $host'
            echo '  TIME: '\$(date)
            echo '════════════════════════════════════════════════════════════════'
            echo ''
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 ${USERNAME}@${host} '$command' 2>&1 | tee '$log_file'
            echo ''
            echo '════════════════════════════════════════════════════════════════'
            echo '  Done. Log: $log_file'
            echo '════════════════════════════════════════════════════════════════'
        "
    done

    echo ""
    echo -e "${GREEN}All nodes started in screen session.${NC}"
    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop all nodes${NC}"
    echo -e "To view output: ${CYAN}screen -r $SESSION_NAME${NC}"
    echo ""
    echo -e "${YELLOW}Screen controls (after attaching):${NC}"
    echo "  Ctrl-a \"     List all windows (shows node names)"
    echo "  Ctrl-a n/p   Next/previous window"
    echo "  Ctrl-a d     Detach (return here)"
    echo ""

    # Store for cleanup
    CLEANUP_NODES=("${nodes[@]}")
    CLEANUP_SESSION="$SESSION_NAME"
    CLEANUP_LOG_DIR="$LOG_DIR"

    cleanup() {
        echo ""
        echo -e "${RED}Caught interrupt, stopping all nodes...${NC}"
        for node in "${CLEANUP_NODES[@]}"; do
            local host="${node}.${DOMAIN}"
            echo -ne "${YELLOW}[$node]${NC} "
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes \
                "${USERNAME}@${host}" "pkill -f 'cargo run --release'" 2>/dev/null &&
                echo -e "${GREEN}killed${NC}" || echo -e "${CYAN}no process${NC}"
        done
        screen -S "$CLEANUP_SESSION" -X quit 2>/dev/null || true
        echo ""
        echo -e "${GREEN}=== Cleanup Complete ===${NC}"
        echo -e "Logs: ${BLUE}$CLEANUP_LOG_DIR${NC}"
        ls -la "$CLEANUP_LOG_DIR"/*.log 2>/dev/null || echo "No logs found"
        exit 0
    }

    trap cleanup SIGINT SIGTERM

    # Wait until Ctrl+C
    while true; do sleep 1; done
}

# Run command
cmd_run() {
    local num_nodes="$1"
    local project_dir="$2"
    shift 2
    local program_args="$*"

    echo -e "${GREEN}=== Running on Cluster ===${NC}"
    echo ""

    mapfile -t nodes < <(select_nodes "$num_nodes")

    echo ""
    echo -e "Directory: ${BLUE}$project_dir${NC}"
    echo -e "Args: ${BLUE}$program_args${NC}"

    local cmd="cd $project_dir && cargo run --release"
    if [[ -n "$program_args" ]]; then
        cmd="$cmd -- $program_args"
    fi

    run_on_nodes "$cmd" "${nodes[@]}"
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
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
    -n)
        NUM_NODES="$2"
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
    cmd_run "$NUM_NODES" "$PROJECT_DIR" $EXTRA_ARGS
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
-h | --help)
    usage
    ;;
*)
    echo -e "${RED}Unknown command: $COMMAND${NC}"
    echo ""
    usage
    ;;
esac
