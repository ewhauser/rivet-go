// Command report turns raw load-generator JSON into bench/RESULTS.md.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

type result struct {
	SDK                 string            `json:"sdk"`
	Variant             string            `json:"variant"`
	Scenario            string            `json:"scenario"`
	Repetition          int               `json:"repetition"`
	StartedAt           string            `json:"started_at"`
	WarmupSeconds       float64           `json:"warmup_seconds"`
	RequestedSeconds    float64           `json:"requested_measure_seconds"`
	TargetOperations    int               `json:"target_operations"`
	MeasuredSeconds     float64           `json:"measured_seconds"`
	Operations          int64             `json:"operations"`
	ThroughputOpsSecond float64           `json:"throughput_ops_second"`
	Latency             latencySummary    `json:"latency_ms"`
	Errors              int64             `json:"errors"`
	WarmupErrors        int64             `json:"warmup_errors"`
	Correctness         correctnessResult `json:"correctness"`
	CPU                 cpuResult         `json:"cpu"`
	Valid               bool              `json:"valid"`
}

type latencySummary struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

type correctnessResult struct {
	Expected int64  `json:"expected"`
	Observed int64  `json:"observed"`
	Detail   string `json:"detail"`
	OK       bool   `json:"ok"`
}

type cpuResult struct {
	Engine processSummary `json:"engine"`
	Runner processSummary `json:"runner"`
}

type processSummary struct {
	Samples       int     `json:"samples"`
	AverageCPU    float64 `json:"average_cpu_percent"`
	MaxCPU        float64 `json:"max_cpu_percent"`
	AverageRSSMiB float64 `json:"average_rss_mib"`
	MaxRSSMiB     float64 `json:"max_rss_mib"`
}

type groupKey struct {
	Scenario string
	Variant  string
	SDK      string
}

var scenarioNames = map[string]string{
	"s1": "S1 hot actor actions",
	"s2": "S2 spread actions",
	"s3": "S3 WebSocket echo",
	"s4": "S4 cold start",
}

func main() {
	var input, output, root, engine, archive string
	flag.StringVar(&input, "input", "", "directory containing raw JSON results")
	flag.StringVar(&output, "output", "", "RESULTS.md output path")
	flag.StringVar(&root, "root", ".", "repository root")
	flag.StringVar(&engine, "engine", "", "pinned engine binary")
	flag.StringVar(&archive, "archive", "", "committed archive path relative to the repository")
	flag.Parse()
	if input == "" || output == "" || engine == "" || archive == "" {
		fatal(errors.New("input, output, engine, and archive are required"))
	}
	results, err := readResults(input)
	if err != nil {
		fatal(err)
	}
	if err := validateMatrix(results); err != nil {
		fatal(err)
	}
	document, err := render(results, root, engine, archive)
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(output, []byte(document), 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "report:", err)
	os.Exit(1)
}

func readResults(input string) ([]result, error) {
	entries, err := os.ReadDir(input)
	if err != nil {
		return nil, err
	}
	var results []result
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(input, entry.Name()))
		if err != nil {
			return nil, err
		}
		var item result
		if err := json.Unmarshal(data, &item); err != nil {
			return nil, fmt.Errorf("decode %s: %w", entry.Name(), err)
		}
		if item.Repetition == 1 || item.Repetition == 2 {
			results = append(results, item)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return resultLess(results[i], results[j])
	})
	return results, nil
}

func resultLess(a, b result) bool {
	if scenarioOrder(a.Scenario) != scenarioOrder(b.Scenario) {
		return scenarioOrder(a.Scenario) < scenarioOrder(b.Scenario)
	}
	if variantOrder(a.Variant) != variantOrder(b.Variant) {
		return variantOrder(a.Variant) < variantOrder(b.Variant)
	}
	if sdkOrder(a.SDK) != sdkOrder(b.SDK) {
		return sdkOrder(a.SDK) < sdkOrder(b.SDK)
	}
	return a.Repetition < b.Repetition
}

func scenarioOrder(value string) int {
	return slices.Index([]string{"s1", "s2", "s3", "s4"}, value)
}

func variantOrder(value string) int {
	return slices.Index([]string{"persist", "no-persist", "not-applicable"}, value)
}

func sdkOrder(value string) int {
	return slices.Index([]string{"go", "typescript", "rust"}, value)
}

func validateMatrix(results []result) error {
	expected := make(map[groupKey]bool)
	for _, scenario := range []string{"s1", "s2", "s4"} {
		for _, sdk := range []string{"go", "typescript", "rust"} {
			expected[groupKey{Scenario: scenario, Variant: "persist", SDK: sdk}] = true
		}
		for _, sdk := range []string{"typescript", "rust"} {
			expected[groupKey{Scenario: scenario, Variant: "no-persist", SDK: sdk}] = true
		}
	}
	for _, sdk := range []string{"go", "typescript", "rust"} {
		expected[groupKey{Scenario: "s3", Variant: "not-applicable", SDK: sdk}] = true
	}
	groups := groupResults(results)
	for key := range expected {
		runs := groups[key]
		if len(runs) != 2 || runs[0].Repetition != 1 || runs[1].Repetition != 2 {
			return fmt.Errorf("%+v has repetitions %v, want 1 and 2", key, repetitions(runs))
		}
		for _, run := range runs {
			if !run.Valid {
				return fmt.Errorf("%+v repetition %d is invalid", key, run.Repetition)
			}
		}
	}
	if len(groups) != len(expected) {
		for key := range groups {
			if !expected[key] {
				return fmt.Errorf("unexpected result group %+v", key)
			}
		}
	}
	return nil
}

func repetitions(results []result) []int {
	values := make([]int, len(results))
	for i, result := range results {
		values[i] = result.Repetition
	}
	return values
}

func groupResults(results []result) map[groupKey][]result {
	groups := make(map[groupKey][]result)
	for _, item := range results {
		key := groupKey{Scenario: item.Scenario, Variant: item.Variant, SDK: item.SDK}
		groups[key] = append(groups[key], item)
	}
	for key := range groups {
		sort.Slice(groups[key], func(i, j int) bool {
			return groups[key][i].Repetition < groups[key][j].Repetition
		})
	}
	return groups
}

func render(results []result, root, engine, archive string) (string, error) {
	groups := groupResults(results)
	chip := command("sysctl", "-n", "machdep.cpu.brand_string")
	logicalCPU := command("sysctl", "-n", "hw.logicalcpu")
	memoryBytes, _ := strconv.ParseFloat(command("sysctl", "-n", "hw.memsize"), 64)
	osVersion := command("sw_vers", "-productVersion")
	osBuild := command("sw_vers", "-buildVersion")
	engineVersion := strings.ReplaceAll(command(engine, "--version"), "\n", "; ")
	goVersion := command("go", "version")
	nodeVersion := command("node", "--version")
	npmVersion := command("npm", "--version")
	rustVersion := command("rustc", "--version")
	cargoVersion := command("cargo", "--version")
	gitCommit := commandAt(root, "git", "rev-parse", "HEAD")
	tsVersion, tsIntegrity := packageLockVersion(filepath.Join(root, "bench/runner-ts/package-lock.json"), "node_modules/rivetkit")
	rustPin := rustLockPin(filepath.Join(root, "bench/runner-rust/Cargo.lock"))
	ffiChecksum := strings.TrimSpace(readOptional(filepath.Join(root, "internal/ffi/lib/darwin_arm64/checksums.txt")))
	startedAt := earliestStart(results)

	var b strings.Builder
	fmt.Fprintln(&b, "# Rivet runner performance results")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Generated from two sequential repetitions per cell on %s. Raw JSON, logs, process samples, and Go CPU profiles are committed under `%s`.\n", startedAt, archive)
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "## Machine and pins")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "| Item | Value |")
	fmt.Fprintln(&b, "|---|---|")
	fmt.Fprintf(&b, "| Machine | %s; %s logical CPUs; %.0f GiB RAM |\n", chip, logicalCPU, memoryBytes/(1<<30))
	fmt.Fprintf(&b, "| OS | macOS %s (%s) |\n", osVersion, osBuild)
	fmt.Fprintf(&b, "| Engine | `%s` |\n", engineVersion)
	fmt.Fprintf(&b, "| Go | `%s`; runner commit `%s` |\n", goVersion, gitCommit)
	fmt.Fprintf(&b, "| Go native library | committed darwin/arm64 release dylib; `%s` |\n", strings.ReplaceAll(ffiChecksum, "|", "\\|"))
	fmt.Fprintf(&b, "| TypeScript | Node `%s`, npm `%s`, `NODE_ENV=production`, no Node flags; `rivetkit@%s` integrity `%s` |\n", nodeVersion, npmVersion, tsVersion, tsIntegrity)
	fmt.Fprintf(&b, "| Rust | `%s`; `%s`; `rivetkit` %s; `cargo build --release --locked` |\n", rustVersion, cargoVersion, rustPin)
	fmt.Fprintln(&b, "| Logging | error level for all runners |")
	fmt.Fprintln(&b)

	fmt.Fprintln(&b, "## Scenario definitions")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "- **S1 hot actor actions:** concurrency 32, one counter actor, repeated `increment(1)` calls.")
	fmt.Fprintln(&b, "- **S2 spread actions:** concurrency 64, one worker for each of 64 counter actors.")
	fmt.Fprintln(&b, "- **S3 WebSocket echo:** 32 connections to one echo actor; each connection performs sequential 64-byte binary ping-pong round trips.")
	fmt.Fprintln(&b, "- **S4 cold start:** 50 fresh actors, sequentially measured from create request through the first persisted or volatile `increment(1)` result. S4 is count-bounded because pacing 50 samples to 60 seconds would fabricate throughput.")
	fmt.Fprintln(&b, "- S1-S3 use at least 10 seconds of excluded warmup and a 60-second measured window. S4 uses at least 10 seconds of excluded fresh-actor warmup and then exactly 50 measured actors. Latency uses an HDR histogram with three significant figures. All requests use the same Go HTTP/WebSocket gateway client.")
	fmt.Fprintln(&b)

	fmt.Fprintln(&b, "## Summary")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "The `r1/r2 (delta)` cells show both repetitions and the signed percentage change from run 1 to run 2. CPU is process `%CPU`, where 100% is one fully occupied logical core.")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, summaryTable(groups))
	fmt.Fprintln(&b)

	for _, scenario := range []string{"s1", "s2", "s3", "s4"} {
		fmt.Fprintf(&b, "## %s\n\n", scenarioNames[scenario])
		fmt.Fprintln(&b, "| SDK | Persistence | Run | Operations | Throughput ops/s | p50 ms | p95 ms | p99 ms | max ms | Errors | Correct | Engine CPU avg/max | Runner CPU avg/max | Runner RSS avg/max MiB |")
		fmt.Fprintln(&b, "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|")
		for _, item := range results {
			if item.Scenario != scenario {
				continue
			}
			fmt.Fprintf(&b, "| %s | %s | %d | %d | %.1f | %.3f | %.3f | %.3f | %.3f | %d | %t (%d/%d) | %.1f/%.1f | %.1f/%.1f | %.1f/%.1f |\n",
				displaySDK(item.SDK), item.Variant, item.Repetition, item.Operations,
				item.ThroughputOpsSecond, item.Latency.P50, item.Latency.P95,
				item.Latency.P99, item.Latency.Max, item.Errors+item.WarmupErrors,
				item.Correctness.OK, item.Correctness.Observed, item.Correctness.Expected,
				item.CPU.Engine.AverageCPU, item.CPU.Engine.MaxCPU,
				item.CPU.Runner.AverageCPU, item.CPU.Runner.MaxCPU,
				item.CPU.Runner.AverageRSSMiB, item.CPU.Runner.MaxRSSMiB)
		}
		fmt.Fprintln(&b)
	}

	fmt.Fprintln(&b, "## Caveats")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "- **Persistence is labeled, not assumed.** The strict `persist` rows await a state save before returning the increment result in every SDK. Go's public action adapter performs this save automatically after a successful handler, so an additional `ctx.Save` would double-save. TypeScript's state proxy normally requests deferred persistence; this actor also awaits `saveState({ immediate: true })`. Rust explicitly awaits `Ctx::save_state`. The `no-persist` rows use actor-generation-local values and exist only for TypeScript and Rust because Go exposes no no-persist successful action.")
	fmt.Fprintln(&b, "- **The native paths differ.** Go crosses a purego C ABI and MessagePack event-pump hop for each event before using the pinned Rust core. TypeScript crosses N-API between JavaScript and the same core and performs JavaScript/CBOR work. Rust calls the core natively. Those costs are the SDK implementations being measured, but this is not a language-only comparison.")
	fmt.Fprintln(&b, "- **Pinned Rust needs the database marker for state.** The standalone git dependency enables `sqlite-remote`, but its registry selects that backend only when `Actor::HAS_DATABASE` is true. Both Rust actors set the marker and issue no application SQL. Omitting it makes new actors fail with `SQLite is unavailable` at this pin.")
	fmt.Fprintln(&b, "- **The client path is neutral.** One Go load generator talks only to the engine gateway over loopback HTTP and WebSockets. It never imports a Go, TypeScript, or Rust actor client.")
	fmt.Fprintln(&b, "- **The gateway IP limiter is sharded, not removed.** Engine v2.3.10 hard-codes 10,000 requests/minute per client IP and trusts `X-Forwarded-For` as a reverse-proxy input. Each HTTP load worker uses one stable loopback identity, identically for every SDK, so that abuse-control ceiling does not cap the runner test. Every non-2xx response remains an error.")
	fmt.Fprintln(&b, "- **Correctness gates validity.** Measured and warmup errors must both be zero. Counter totals are reconciled after S1/S2, every S4 first result must be 1, and every S3 payload must match. Invalid cells are rejected by the report generator.")
	fmt.Fprintln(&b, "- **Freshness and ordering.** The engine is restarted with a new filesystem data directory before each SDK suite. Variants and repetitions within an SDK share that suite's engine process but use fresh uniquely keyed actors. All benchmark invocations are sequential.")
	fmt.Fprintln(&b, "- **S4 is deliberately count-bounded.** It reports exactly the requested 50 fresh actors. Its actual elapsed duration and throughput are reported; a forced 60-second pacing window would measure the pacer rather than cold start.")
	fmt.Fprintln(&b, "- **CPU attribution is sampled.** Engine and runner `%CPU`/RSS come from one-second `ps` samples during the measured interval. A process near 100% may be saturating one core even when the whole machine has idle cores. The report flags likely engine-limited rows below.")
	fmt.Fprintln(&b, "- **Single-machine loopback only.** These values include the engine and local transport on one macOS host. They do not predict networked or multi-host deployments, and the script cannot prove that unrelated host activity or thermal state was identical between suites.")
	fmt.Fprintln(&b, "- **Concurrency is part of the SDK behavior.** The same gateway concurrency is offered to every runner. Go dispatches one serialized actor worker; pinned Rust action futures are spawned onto Tokio, and TypeScript callbacks may overlap across awaited native work. The benchmark does not add user locks that would hide those SDK semantics.")
	fmt.Fprintln(&b)

	fmt.Fprintln(&b, "### Likely engine-limited cells")
	fmt.Fprintln(&b)
	limited := engineLimited(results)
	if len(limited) == 0 {
		fmt.Fprintln(&b, "No row averaged at least 90% engine CPU while the engine also consumed more CPU than its runner. This does not prove the engine was never the latency bottleneck.")
	} else {
		fmt.Fprintln(&b, "These repetitions averaged at least 90% engine CPU and more engine CPU than runner CPU; treat runner-to-runner differences there as potentially engine-capped:")
		fmt.Fprintln(&b)
		for _, text := range limited {
			fmt.Fprintf(&b, "- %s\n", text)
		}
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "## Go CPU profiles")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Profiling-only S1 and S3 runs are excluded from every table above. Their pprof data and text tops are in `%s/go-s1-cpu.pprof`, `%s/go-s3-cpu.pprof`, and the adjacent `*-pprof-top.txt` files.\n", archive, archive)

	return b.String(), nil
}

func summaryTable(groups map[groupKey][]result) string {
	keys := make([]groupKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a := result{Scenario: keys[i].Scenario, Variant: keys[i].Variant, SDK: keys[i].SDK}
		b := result{Scenario: keys[j].Scenario, Variant: keys[j].Variant, SDK: keys[j].SDK}
		return resultLess(a, b)
	})
	var b strings.Builder
	fmt.Fprintln(&b, "| Scenario | SDK | Persistence | Throughput ops/s r1/r2 (delta) | p50 ms r1/r2 (delta) | p95 ms r1/r2 (delta) | p99 ms r1/r2 (delta) | max ms r1/r2 | Errors r1/r2 | Engine CPU avg r1/r2 | Runner CPU avg r1/r2 | Valid |")
	fmt.Fprintln(&b, "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, key := range keys {
		runs := groups[key]
		a, c := runs[0], runs[1]
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %.3f/%.3f | %d/%d | %.1f/%.1f | %.1f/%.1f | %t/%t |\n",
			strings.ToUpper(key.Scenario), displaySDK(key.SDK), key.Variant,
			pairDelta(a.ThroughputOpsSecond, c.ThroughputOpsSecond, 1),
			pairDelta(a.Latency.P50, c.Latency.P50, 3),
			pairDelta(a.Latency.P95, c.Latency.P95, 3),
			pairDelta(a.Latency.P99, c.Latency.P99, 3),
			a.Latency.Max, c.Latency.Max,
			a.Errors+a.WarmupErrors, c.Errors+c.WarmupErrors,
			a.CPU.Engine.AverageCPU, c.CPU.Engine.AverageCPU,
			a.CPU.Runner.AverageCPU, c.CPU.Runner.AverageCPU,
			a.Valid, c.Valid)
	}
	return strings.TrimRight(b.String(), "\n")
}

func pairDelta(a, b float64, decimals int) string {
	format := fmt.Sprintf("%%.%df/%%.%df (%%+.1f%%%%)", decimals, decimals)
	return fmt.Sprintf(format, a, b, percentDelta(a, b))
}

func percentDelta(a, b float64) float64 {
	if a == 0 {
		return 0
	}
	return (b - a) / a * 100
}

func displaySDK(sdk string) string {
	switch sdk {
	case "go":
		return "Go"
	case "typescript":
		return "TypeScript"
	case "rust":
		return "Rust"
	default:
		return sdk
	}
}

func engineLimited(results []result) []string {
	var limited []string
	for _, item := range results {
		engineCPU := item.CPU.Engine.AverageCPU
		runnerCPU := item.CPU.Runner.AverageCPU
		if engineCPU >= 90 && engineCPU > runnerCPU {
			limited = append(limited, fmt.Sprintf("%s %s %s run %d: engine %.1f%%, runner %.1f%%", strings.ToUpper(item.Scenario), displaySDK(item.SDK), item.Variant, item.Repetition, engineCPU, runnerCPU))
		}
	}
	return limited
}

func earliestStart(results []result) string {
	var earliest time.Time
	for _, item := range results {
		parsed, err := time.Parse(time.RFC3339, item.StartedAt)
		if err != nil {
			continue
		}
		if earliest.IsZero() || parsed.Before(earliest) {
			earliest = parsed
		}
	}
	if earliest.IsZero() {
		return "unknown time"
	}
	return earliest.Format(time.RFC3339)
}

func packageLockVersion(path, packagePath string) (string, string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "unknown", "unknown"
	}
	var lock struct {
		Packages map[string]struct {
			Version   string `json:"version"`
			Integrity string `json:"integrity"`
		} `json:"packages"`
	}
	if json.Unmarshal(data, &lock) != nil {
		return "unknown", "unknown"
	}
	entry := lock.Packages[packagePath]
	return entry.Version, entry.Integrity
}

func rustLockPin(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "unknown"
	}
	re := regexp.MustCompile(`(?ms)\[\[package\]\]\s+name = "rivetkit"\s+version = "([^"]+)"\s+source = "([^"]+)"`)
	match := re.FindStringSubmatch(string(data))
	if len(match) != 3 {
		return "unknown"
	}
	return fmt.Sprintf("v%s from `%s`", match[1], match[2])
}

func command(name string, args ...string) string {
	output, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("unavailable (%v)", err)
	}
	return strings.TrimSpace(string(output))
}

func commandAt(directory, name string, args ...string) string {
	cmd := exec.Command(name, args...)
	cmd.Dir = directory
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("unavailable (%v)", err)
	}
	return strings.TrimSpace(string(output))
}

func readOptional(path string) string {
	data, _ := os.ReadFile(path)
	return string(data)
}
