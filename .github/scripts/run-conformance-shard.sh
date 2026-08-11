#!/usr/bin/env bash
set -euo pipefail

readonly shard="${1:-}"

readonly scheduling_tests=(
  TestSchedulingSleepAndMidflightPolicy
  TestAlarmSurvivesEngineRestart
  TestActionImplicitSavePersistsAcrossEngineRestart
  TestRunnerReportsEngineDisconnect
  TestDefaultWebSocketClosesOnSleep
  TestActorDestroyRunsGoCleanupHook
  TestRunnerRegistersWithEngine
)

readonly state_tests=(
  TestDurableActionSchedulesAcrossSleepAndRunnerRestart
  TestPerActorSQLiteConformance
  TestPortedRunnableExamples
  TestCounterStatePersistsAcrossEngineRestart
  TestDurableQueuesAndManagedWorkAcrossGenerations
  TestNativeBoundaryConcurrencyAndLifecycle
  TestContextDestroyOwnsGenerationTeardown
  TestPanickingActorDoesNotKillRunner
  TestPublicActorIdentityAndKV
  TestTwoActorsOnOneRunnerHaveIsolatedState
)

readonly transport_tests=(
  TestActionsAndHTTPTunnelRoundTrip
  TestDatabaseActorLiveGenerationDoesNotRehydrateAcrossEngineCrash
  TestActorConnectRejectedStateDoesNotPersistActorMutation
  TestWebSocketStateSavePersistsAcrossEngineRestart
  TestHibernatingWebSocketSurvivesSleep
  TestRunnerNewFailuresAreStructuredAndBounded
  TestNativeKVListCrossesPollBatchBoundaryAndReportsErrors
  TestRunnableExamplesAndSIGTERMDrain
  TestWebSocketsAndActorEvents
  TestActorConnectConnectionContextPersistsAcrossSleep
  TestWebSocketHookPanicsStopOnlyTheirActor
  TestWebSocketLifecycleRacesAndHookBroadcasts
)

readonly declared_tests=(
  "${scheduling_tests[@]}"
  "${state_tests[@]}"
  "${transport_tests[@]}"
)

discovered_tests=()
while IFS= read -r test_name; do
  discovered_tests+=("$test_name")
done < <(go test -list '^Test' ./conformance | sed -n '/^Test/p')

failed=0
for test_name in "${declared_tests[@]}"; do
  declared_count=0
  discovered_count=0
  for candidate in "${declared_tests[@]}"; do
    if [[ "$candidate" == "$test_name" ]]; then
      declared_count=$((declared_count + 1))
    fi
  done
  for candidate in "${discovered_tests[@]}"; do
    if [[ "$candidate" == "$test_name" ]]; then
      discovered_count=$((discovered_count + 1))
    fi
  done
  if (( declared_count != 1 )); then
    echo "conformance test $test_name is assigned $declared_count times" >&2
    failed=1
  fi
  if (( discovered_count != 1 )); then
    echo "assigned conformance test $test_name was not discovered" >&2
    failed=1
  fi
done

for test_name in "${discovered_tests[@]}"; do
  declared_count=0
  for candidate in "${declared_tests[@]}"; do
    if [[ "$candidate" == "$test_name" ]]; then
      declared_count=$((declared_count + 1))
    fi
  done
  if (( declared_count != 1 )); then
    echo "discovered conformance test $test_name is assigned $declared_count times" >&2
    failed=1
  fi
done

if (( failed != 0 )); then
  echo "update .github/scripts/run-conformance-shard.sh so every top-level conformance test runs exactly once" >&2
  exit 1
fi

case "$shard" in
  check)
    echo "all ${#declared_tests[@]} top-level conformance tests are assigned exactly once"
    exit 0
    ;;
  scheduling)
    selected_tests=("${scheduling_tests[@]}")
    ;;
  state)
    selected_tests=("${state_tests[@]}")
    ;;
  transport)
    selected_tests=("${transport_tests[@]}")
    ;;
  *)
    echo "usage: $0 {check|scheduling|state|transport}" >&2
    exit 2
    ;;
esac

pattern='^('
separator=''
for test_name in "${selected_tests[@]}"; do
  pattern+="$separator$test_name"
  separator='|'
done
pattern+=')$'

echo "running conformance shard $shard (${#selected_tests[@]} tests)"
exec go test -race -count=1 -timeout=12m -v ./conformance -run "$pattern"
