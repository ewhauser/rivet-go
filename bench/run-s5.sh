#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BENCH_DIR="$ROOT_DIR/bench"
BIN_DIR="$BENCH_DIR/.bin"
RUN_STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_DIR="$BENCH_DIR/results/$RUN_STAMP-sqlite"
RAW_DIR="$RUN_DIR/raw"
LOG_DIR="$RUN_DIR/logs"
WORK_DIR="$RUN_DIR/work"
LOCAL_DATE=$(TZ=America/Denver date +%F)
ARCHIVE_LABEL=${BENCH_ARCHIVE_DATE:-$LOCAL_DATE-sqlite}
ARCHIVE_DIR="$BENCH_DIR/results-archive/$ARCHIVE_LABEL"
ENGINE_BIN=${RIVET_GO_ENGINE_BIN:-$HOME/.cache/rivet-go/engine-v2.3.10/rivet-engine}
ENGINE_PORT=${BENCH_ENGINE_PORT:-16420}
ENDPOINT="http://127.0.0.1:$ENGINE_PORT"

GO_RUNNER="$BIN_DIR/runner-go"
LOADGEN="$BIN_DIR/loadgen"
REPORT="$BIN_DIR/report-s5"
TS_RUNNER="$BENCH_DIR/runner-ts/dist/index.js"

ENGINE_PID=""
RUNNER_PID=""

mkdir -p "$BIN_DIR" "$RAW_DIR" "$LOG_DIR" "$WORK_DIR"

stop_runner() {
	if [[ -n "$RUNNER_PID" ]] && kill -0 "$RUNNER_PID" 2>/dev/null; then
		kill -TERM "$RUNNER_PID" 2>/dev/null || true
		wait "$RUNNER_PID" 2>/dev/null || true
	fi
	RUNNER_PID=""
}

stop_engine() {
	if [[ -n "$ENGINE_PID" ]] && kill -0 "$ENGINE_PID" 2>/dev/null; then
		kill -INT "$ENGINE_PID" 2>/dev/null || true
		for _ in {1..100}; do
			if ! kill -0 "$ENGINE_PID" 2>/dev/null; then
				break
			fi
			sleep 0.1
		done
		if kill -0 "$ENGINE_PID" 2>/dev/null; then
			kill -KILL "$ENGINE_PID" 2>/dev/null || true
		fi
		wait "$ENGINE_PID" 2>/dev/null || true
	fi
	ENGINE_PID=""
}

cleanup() {
	stop_runner
	stop_engine
}
trap cleanup EXIT INT TERM

wait_http() {
	local target=$1
	for _ in {1..300}; do
		if curl --silent --fail --max-time 1 "$target" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.1
	done
	return 1
}

wait_runner() {
	local runner_name=$1
	for _ in {1..300}; do
		if [[ -n "$RUNNER_PID" ]] && ! kill -0 "$RUNNER_PID" 2>/dev/null; then
			return 1
		fi
		if curl --silent --fail --max-time 1 -H "Authorization: Bearer dev" "$ENDPOINT/envoys?namespace=default&name=$runner_name" | grep -q "$runner_name"; then
			return 0
		fi
		sleep 0.1
	done
	return 1
}

start_engine() {
	local suite=$1
	local data_dir="$WORK_DIR/engine-$suite"
	mkdir -p "$data_dir"
	env \
		RIVET__GUARD__HOST=127.0.0.1 \
		RIVET__GUARD__PORT="$ENGINE_PORT" \
		RIVET__API_PEER__HOST=127.0.0.1 \
		RIVET__API_PEER__PORT="$((ENGINE_PORT + 1))" \
		RIVET__METRICS__HOST=127.0.0.1 \
		RIVET__METRICS__PORT="$((ENGINE_PORT + 10))" \
		RIVET__FILE_SYSTEM__PATH="$data_dir/db" \
		"$ENGINE_BIN" start >"$LOG_DIR/engine-$suite.log" 2>&1 &
	ENGINE_PID=$!
	if ! wait_http "$ENDPOINT/health"; then
		tail -n 80 "$LOG_DIR/engine-$suite.log" >&2
		return 1
	fi
}

start_runner() {
	local suite=$1
	local runner_name=$2
	local log_path="$LOG_DIR/runner-$suite.log"
	case "$suite" in
		go-ffi|go-socket)
			local transport=${suite#go-}
			env \
				RIVET_ENDPOINT="$ENDPOINT" \
				RIVET_GO_SQLITE_TRANSPORT="$transport" \
				BENCH_RUNNER_NAME="$runner_name" \
				BENCH_PERSIST_MODE=persist \
				"$GO_RUNNER" >"$log_path" 2>&1 &
			RUNNER_PID=$!
			;;
		typescript)
			(
				cd "$BENCH_DIR/runner-ts"
				exec env \
					NODE_ENV=production \
					RIVET_ENDPOINT="$ENDPOINT" \
					RIVET_RUN_ENGINE=0 \
					RIVET_LOG_LEVEL=error \
					BENCH_RUNNER_NAME="$runner_name" \
					BENCH_PERSIST_MODE=persist \
					node "$TS_RUNNER"
			) >"$log_path" 2>&1 &
			RUNNER_PID=$!
			;;
	esac
	if ! wait_runner "$runner_name"; then
		tail -n 80 "$log_path" >&2
		return 1
	fi
}

run_suite() {
	local suite=$1
	local variant=$2
	local runner_name="bench-$suite-s5"
	echo "Running S5 $suite with a fresh engine data directory"
	start_engine "$suite"
	start_runner "$suite" "$runner_name"
	for repetition in 1 2; do
		"$LOADGEN" \
			-endpoint "$ENDPOINT" \
			-runner-name "$runner_name" \
			-sdk "$suite" \
			-variant "$variant" \
			-scenario s5 \
			-repetition "$repetition" \
			-warmup 10s \
			-measure 45s \
			-engine-pid "$ENGINE_PID" \
			-runner-pid "$RUNNER_PID" \
			-output "$RAW_DIR/s5-$suite-$variant-r$repetition.json"
	done
	stop_runner
	stop_engine
}

if [[ ! -x "$ENGINE_BIN" ]]; then
	echo "pinned engine is not executable: $ENGINE_BIN" >&2
	exit 1
fi
if curl --silent --fail --max-time 1 "$ENDPOINT/health" >/dev/null 2>&1; then
	echo "benchmark endpoint is already in use: $ENDPOINT" >&2
	exit 1
fi
if [[ -e "$ARCHIVE_DIR" ]]; then
	echo "archive already exists: $ARCHIVE_DIR" >&2
	exit 1
fi

echo "Building S5 benchmark binaries"
go build -trimpath -o "$GO_RUNNER" ./bench/runner-go
go build -trimpath -o "$LOADGEN" ./bench/loadgen
go build -trimpath -o "$REPORT" ./bench/report-s5
npm ci --prefix "$BENCH_DIR/runner-ts"
npm run --prefix "$BENCH_DIR/runner-ts" build

run_suite go-ffi ffi
run_suite go-socket socket
run_suite typescript raw-sql

mkdir -p "$ARCHIVE_DIR"
cp "$RAW_DIR"/*.json "$ARCHIVE_DIR/"
cp "$LOG_DIR"/*.log "$ARCHIVE_DIR/"
{
	"$ENGINE_BIN" --version
	go version
	node --version
	npm --version
	sw_vers
	sysctl -n machdep.cpu.brand_string
	sysctl -n hw.memsize
	git -C "$ROOT_DIR" rev-parse HEAD
	shasum -a 256 "$ROOT_DIR/internal/ffi/lib/darwin_arm64/librivetkit_go_ffi.dylib"
} >"$ARCHIVE_DIR/environment.txt"

"$REPORT" \
	-input "$RAW_DIR" \
	-output "$BENCH_DIR/RESULTS.md" \
	-archive "bench/results-archive/$ARCHIVE_LABEL"

(
	cd "$ARCHIVE_DIR"
	shasum -a 256 ./* >SHA256SUMS
)

echo "Results: $BENCH_DIR/RESULTS.md"
echo "Archive: $ARCHIVE_DIR"
