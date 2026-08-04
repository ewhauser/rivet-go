#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BENCH_DIR="$ROOT_DIR/bench"
BIN_DIR="$BENCH_DIR/.bin"
RUN_STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN_DIR="$BENCH_DIR/results/$RUN_STAMP"
RAW_DIR="$RUN_DIR/raw"
PROFILE_DIR="$RUN_DIR/profiles"
LOG_DIR="$RUN_DIR/logs"
WORK_DIR="$RUN_DIR/work"
ARCHIVE_DATE=${BENCH_ARCHIVE_DATE:-$(date +%F)}
ARCHIVE_DIR="$BENCH_DIR/results-archive/$ARCHIVE_DATE"
ENGINE_BIN=${RIVET_GO_ENGINE_BIN:-$HOME/.cache/rivet-go/engine-v2.3.10/rivet-engine}
ENGINE_PORT=${BENCH_ENGINE_PORT:-16420}
ENDPOINT="http://127.0.0.1:$ENGINE_PORT"
WARMUP_SECONDS=${BENCH_WARMUP_SECONDS:-10}
MEASURE_SECONDS=${BENCH_MEASURE_SECONDS:-60}
REPETITIONS=${BENCH_REPETITIONS:-2}
PPROF_PORT=${BENCH_PPROF_PORT:-16060}

GO_RUNNER="$BIN_DIR/runner-go"
LOADGEN="$BIN_DIR/loadgen"
REPORT="$BIN_DIR/report"
TS_RUNNER="$BENCH_DIR/runner-ts/dist/index.js"
RUST_RUNNER="$BENCH_DIR/runner-rust/target/release/rivet-go-bench-runner-rust"

ENGINE_PID=""
RUNNER_PID=""

mkdir -p "$BIN_DIR" "$RAW_DIR" "$PROFILE_DIR" "$LOG_DIR" "$WORK_DIR"

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
	local url=$1
	for _ in {1..300}; do
		if curl --silent --fail --max-time 1 "$url" >/dev/null 2>&1; then
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
		if curl --silent --fail --max-time 1 \
			-H "Authorization: Bearer dev" \
			"$ENDPOINT/envoys?namespace=default&name=$runner_name" \
			| grep -q "$runner_name"; then
			return 0
		fi
		sleep 0.1
	done
	return 1
}

start_engine() {
	local sdk=$1
	local data_dir="$WORK_DIR/engine-$sdk"
	mkdir -p "$data_dir"
	env \
		RIVET__GUARD__HOST=127.0.0.1 \
		RIVET__GUARD__PORT="$ENGINE_PORT" \
		RIVET__API_PEER__HOST=127.0.0.1 \
		RIVET__API_PEER__PORT="$((ENGINE_PORT + 1))" \
		RIVET__METRICS__HOST=127.0.0.1 \
		RIVET__METRICS__PORT="$((ENGINE_PORT + 10))" \
		RIVET__FILE_SYSTEM__PATH="$data_dir/db" \
		"$ENGINE_BIN" start >"$LOG_DIR/engine-$sdk.log" 2>&1 &
	ENGINE_PID=$!
	if ! wait_http "$ENDPOINT/health"; then
		tail -n 80 "$LOG_DIR/engine-$sdk.log" >&2
		return 1
	fi
}

start_runner() {
	local sdk=$1
	local variant=$2
	local runner_name=$3
	local log_path="$LOG_DIR/runner-$sdk-$variant.log"
	case "$sdk" in
		go)
			env \
				RIVET_ENDPOINT="$ENDPOINT" \
				BENCH_RUNNER_NAME="$runner_name" \
				BENCH_PERSIST_MODE="$variant" \
				BENCH_PPROF_ADDR="127.0.0.1:$PPROF_PORT" \
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
					BENCH_PERSIST_MODE="$variant" \
					node "$TS_RUNNER"
			) >"$log_path" 2>&1 &
			RUNNER_PID=$!
		;;
		rust)
			env \
				RUST_LOG=error \
				RIVET_ENDPOINT="$ENDPOINT" \
				RIVET_TOKEN=dev \
				RIVET_NAMESPACE=default \
				RIVET_POOL_NAME="$runner_name" \
				RIVET_RUN_ENGINE=0 \
				BENCH_PERSIST_MODE="$variant" \
				"$RUST_RUNNER" >"$log_path" 2>&1 &
			RUNNER_PID=$!
		;;
		*)
			return 1
		;;
	esac
	if ! wait_runner "$runner_name"; then
		tail -n 80 "$log_path" >&2
		return 1
	fi
}

run_cell() {
	local sdk=$1
	local result_variant=$2
	local scenario=$3
	local repetition=$4
	local runner_name=$5
	local output="$RAW_DIR/$scenario-$sdk-$result_variant-r$repetition.json"
	"$LOADGEN" \
		-endpoint "$ENDPOINT" \
		-runner-name "$runner_name" \
		-sdk "$sdk" \
		-variant "$result_variant" \
		-scenario "$scenario" \
		-repetition "$repetition" \
		-warmup "${WARMUP_SECONDS}s" \
		-measure "${MEASURE_SECONDS}s" \
		-cold-actors 50 \
		-engine-pid "$ENGINE_PID" \
		-runner-pid "$RUNNER_PID" \
		-output "$output"
}

run_mode() {
	local sdk=$1
	local variant=$2
	local runner_name="bench-$sdk-$variant"
	start_runner "$sdk" "$variant" "$runner_name"
	local scenarios=(s1 s2 s4)
	if [[ "$variant" == "persist" ]]; then
		scenarios=(s1 s2 s3 s4)
	fi
	for scenario in "${scenarios[@]}"; do
		local result_variant=$variant
		if [[ "$scenario" == "s3" ]]; then
			result_variant=not-applicable
		fi
		for repetition in $(seq 1 "$REPETITIONS"); do
			run_cell "$sdk" "$result_variant" "$scenario" "$repetition" "$runner_name"
		done
	done
	if [[ "$sdk" == "go" && "$variant" == "persist" && "$REPETITIONS" == "2" ]]; then
		if ! wait_http "http://127.0.0.1:$PPROF_PORT/debug/pprof/"; then
			return 1
		fi
		for scenario in s1 s3; do
			"$LOADGEN" \
				-endpoint "$ENDPOINT" \
				-runner-name "$runner_name" \
				-sdk go-profile \
				-variant not-applicable \
				-scenario "$scenario" \
				-repetition 99 \
				-warmup "${WARMUP_SECONDS}s" \
				-measure 35s \
				-engine-pid "$ENGINE_PID" \
				-runner-pid "$RUNNER_PID" \
				-pprof-url "http://127.0.0.1:$PPROF_PORT" \
				-pprof-out "$PROFILE_DIR/go-$scenario-cpu.pprof" \
				-pprof-seconds 30 \
				-output "$PROFILE_DIR/go-$scenario-profile-run.json"
		done
	fi
	stop_runner
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
	echo "archive already exists: $ARCHIVE_DIR (set BENCH_ARCHIVE_DATE to a unique date-like label)" >&2
	exit 1
fi
if [[ "$REPETITIONS" != "2" ]]; then
	echo "BENCH_REPETITIONS must be 2 for a reportable full evaluation" >&2
	exit 1
fi

echo "Building benchmark binaries"
go build -trimpath -o "$GO_RUNNER" ./bench/runner-go
go build -trimpath -o "$LOADGEN" ./bench/loadgen
go build -trimpath -o "$REPORT" ./bench/report
npm ci --prefix "$BENCH_DIR/runner-ts"
npm run --prefix "$BENCH_DIR/runner-ts" build
cargo build --manifest-path "$BENCH_DIR/runner-rust/Cargo.toml" --release --locked

for sdk in go typescript rust; do
	echo "Running $sdk suite with a fresh engine data directory"
	start_engine "$sdk"
	run_mode "$sdk" persist
	if [[ "$sdk" != "go" ]]; then
		run_mode "$sdk" no-persist
	fi
	stop_engine
done

mkdir -p "$ARCHIVE_DIR"
cp "$RAW_DIR"/*.json "$ARCHIVE_DIR/"
cp "$PROFILE_DIR"/* "$ARCHIVE_DIR/"
cp "$LOG_DIR"/*.log "$ARCHIVE_DIR/"
go tool pprof -top -nodecount=25 "$GO_RUNNER" "$PROFILE_DIR/go-s1-cpu.pprof" >"$ARCHIVE_DIR/go-s1-pprof-top.txt"
go tool pprof -top -nodecount=25 "$GO_RUNNER" "$PROFILE_DIR/go-s3-cpu.pprof" >"$ARCHIVE_DIR/go-s3-pprof-top.txt"

{
	"$ENGINE_BIN" --version
	go version
	node --version
	npm --version
	rustc --version
	cargo --version
	sw_vers
	sysctl -n machdep.cpu.brand_string
	sysctl -n hw.memsize
	git -C "$ROOT_DIR" rev-parse HEAD
} >"$ARCHIVE_DIR/environment.txt"

"$REPORT" \
	-input "$RAW_DIR" \
	-output "$BENCH_DIR/RESULTS.md" \
	-root "$ROOT_DIR" \
	-engine "$ENGINE_BIN" \
	-archive "bench/results-archive/$ARCHIVE_DATE"

(
	cd "$ARCHIVE_DIR"
	shasum -a 256 ./* >SHA256SUMS
)

echo "Results: $BENCH_DIR/RESULTS.md"
echo "Archive: $ARCHIVE_DIR"
