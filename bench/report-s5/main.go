// Command report-s5 appends the M7 SQLite transport candidate section to the
// existing benchmark report without rewriting the M6 scenario history.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	ThroughputOpsSecond float64           `json:"throughput_ops_second"`
	Latency             latencySummary    `json:"latency_ms"`
	Errors              int64             `json:"errors"`
	WarmupErrors        int64             `json:"warmup_errors"`
	Correctness         correctnessResult `json:"correctness"`
	CPU                 cpuResult         `json:"cpu"`
	Valid               bool              `json:"valid"`
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type correctnessResult struct {
	Expected int64 `json:"expected"`
	Observed int64 `json:"observed"`
	OK       bool  `json:"ok"`
}

type cpuResult struct {
	Engine processSummary `json:"engine"`
	Runner processSummary `json:"runner"`
}

type processSummary struct {
	Samples    int     `json:"samples"`
	AverageCPU float64 `json:"average_cpu_percent"`
}

const sectionHeading = "## S5 per-actor SQLite transport candidates"

func main() {
	var input, output, archive string
	flag.StringVar(&input, "input", "", "directory containing S5 raw JSON")
	flag.StringVar(&output, "output", "", "existing RESULTS.md")
	flag.StringVar(&archive, "archive", "", "archive path relative to repository")
	flag.Parse()
	if input == "" || output == "" || archive == "" {
		fatal(errors.New("input, output, and archive are required"))
	}
	results, err := readResults(input)
	if err != nil {
		fatal(err)
	}
	if err := validate(results); err != nil {
		fatal(err)
	}
	section := render(results, archive)
	existing, err := os.ReadFile(output)
	if err != nil {
		fatal(err)
	}
	document := strings.TrimRight(string(existing), "\n")
	if offset := strings.Index(document, "\n"+sectionHeading); offset >= 0 {
		document = document[:offset]
	}
	document += "\n\n" + section
	if err := os.WriteFile(output, []byte(document), 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "report-s5:", err)
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
		if item.Scenario == "s5" {
			results = append(results, item)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if sdkOrder(results[i].SDK) != sdkOrder(results[j].SDK) {
			return sdkOrder(results[i].SDK) < sdkOrder(results[j].SDK)
		}
		return results[i].Repetition < results[j].Repetition
	})
	return results, nil
}

func sdkOrder(sdk string) int {
	switch sdk {
	case "go-ffi":
		return 0
	case "go-socket":
		return 1
	case "typescript":
		return 2
	default:
		return 99
	}
}

func validate(results []result) error {
	if len(results) != 6 {
		return fmt.Errorf("S5 matrix has %d results, want 6", len(results))
	}
	wantVariant := map[string]string{"go-ffi": "ffi", "go-socket": "socket", "typescript": "raw-sql"}
	seen := make(map[string]bool)
	for _, item := range results {
		variant, exists := wantVariant[item.SDK]
		if !exists || item.Variant != variant || item.Repetition < 1 || item.Repetition > 2 {
			return fmt.Errorf("unexpected S5 cell sdk=%q variant=%q repetition=%d", item.SDK, item.Variant, item.Repetition)
		}
		key := fmt.Sprintf("%s/%d", item.SDK, item.Repetition)
		if seen[key] {
			return fmt.Errorf("duplicate S5 cell %s", key)
		}
		seen[key] = true
		if item.WarmupSeconds != 10 || item.RequestedSeconds != 45 {
			return fmt.Errorf("%s used warmup %.1fs and measure %.1fs, want 10s/45s", key, item.WarmupSeconds, item.RequestedSeconds)
		}
		if !item.Valid || item.Errors != 0 || item.WarmupErrors != 0 || !item.Correctness.OK || item.Correctness.Expected != item.Correctness.Observed {
			return fmt.Errorf("invalid S5 cell %s", key)
		}
		if item.CPU.Engine.Samples == 0 || item.CPU.Runner.Samples == 0 {
			return fmt.Errorf("S5 cell %s has no process samples", key)
		}
	}
	return nil
}

func render(results []result, archive string) string {
	groups := make(map[string][]result)
	var latest time.Time
	for _, item := range results {
		groups[item.SDK] = append(groups[item.SDK], item)
		if parsed, err := time.Parse(time.RFC3339, item.StartedAt); err == nil && parsed.After(latest) {
			latest = parsed
		}
	}
	var b strings.Builder
	fmt.Fprintln(&b, sectionHeading)
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Generated from two sequential repetitions per candidate; the last measured cell started at %s. Raw JSON, process logs, environment data, and checksums are committed under `%s`.\n", latest.UTC().Format(time.RFC3339), archive)
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Each repetition uses 32 workers mapped one-to-one to 32 actors, 10 seconds of excluded warmup, and a 45-second measured window. The deterministic operation cycle is 50% point `SELECT`, 40% single-row `INSERT`, and 10% one transaction containing `INSERT`, `UPDATE`, and `SELECT`. Throughput counts the transaction as one composite operation. Final per-actor row counts must equal the post-warmup baseline plus successful measured inserts.")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "| Runner | Throughput ops/s r1/r2 (avg) | p50 ms r1/r2 | p95 ms r1/r2 | p99 ms r1/r2 | Runner CPU avg r1/r2 | Engine CPU avg r1/r2 | Row reconciliation r1/r2 | Valid |")
	fmt.Fprintln(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, sdk := range []string{"go-ffi", "go-socket", "typescript"} {
		runs := groups[sdk]
		fmt.Fprintf(&b, "| %s | %.1f/%.1f (%.1f) | %.3f/%.3f | %.3f/%.3f | %.3f/%.3f | %.1f%%/%.1f%% | %.1f%%/%.1f%% | %d/%d; %d/%d | true/true |\n",
			displaySDK(sdk),
			runs[0].ThroughputOpsSecond, runs[1].ThroughputOpsSecond, (runs[0].ThroughputOpsSecond+runs[1].ThroughputOpsSecond)/2,
			runs[0].Latency.P50, runs[1].Latency.P50,
			runs[0].Latency.P95, runs[1].Latency.P95,
			runs[0].Latency.P99, runs[1].Latency.P99,
			runs[0].CPU.Runner.AverageCPU, runs[1].CPU.Runner.AverageCPU,
			runs[0].CPU.Engine.AverageCPU, runs[1].CPU.Engine.AverageCPU,
			runs[0].Correctness.Observed, runs[0].Correctness.Expected,
			runs[1].Correctness.Observed, runs[1].Correctness.Expected,
		)
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "All three suites start a fresh engine data directory. Both Go rows use core's `LocalNative` SQLite worker and differ only in the Go-to-core transport. The TypeScript reference uses `rivetkit@2.3.10` `c.db` raw `execute` and callback `transaction` APIs with the same statements and no ORM. The TypeScript wrapper returns object rows and manages the transaction callback, while the Go API returns column/value matrices and exposes an explicit lease-backed `Tx`; those API-shape costs remain part of the measured SDK paths.")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "CPU is sampled process `%CPU`, where 100% is one fully occupied logical core. This section records the candidates without selecting a default.")
	return b.String()
}

func displaySDK(sdk string) string {
	switch sdk {
	case "go-ffi":
		return "Go-ffi"
	case "go-socket":
		return "Go-socket"
	case "typescript":
		return "TypeScript `c.db`"
	default:
		return sdk
	}
}
