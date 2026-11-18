#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 || ( $1 != smoke && $1 != full ) ]]; then
    echo "Usage: ${0##*/} {smoke|full}" >&2
    exit 2
fi
mode=$1
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

toolchain=1.99.1
gateway=(cargo test --locked -p iam-gateway --features profile --test tls)
revision=$(git rev-parse HEAD)
if [[ -n $(git status --porcelain) ]]; then
    revision+=" (dirty worktree)"
fi
printf 'TiDB %s tests at %s\n' "$mode" "$revision"
compiler=$(rustc --version)
echo "$compiler"
if [[ $compiler != "rustc $toolchain "* ]]; then
    echo "Select Rust $toolchain, for example with RUSTUP_TOOLCHAIN=$toolchain." >&2
    exit 1
fi

if [[ $mode == smoke ]]; then
    cases=(
        tidb_tls_recovers_committed_send_after_client_disconnect
        tidb_tls_recovers_last_dropped_push_and_reconnects_after_gateway_restart
        friends::local_release::tidb_tls_local_release_recovers_after_gateway_process_exit
    )
    listed=$("${gateway[@]}" -- --ignored --exact --list "${cases[@]}")
    for test_case in "${cases[@]}"; do
        if ! grep -Fxq -- "$test_case: test" <<< "$listed"; then
            echo "Missing ignored smoke test: $test_case" >&2
            exit 1
        fi
    done
fi

project="iam-tidb-test-$(date +%s)-$$"
compose=(docker compose --project-name "$project" --file scripts/tidb-test.compose.yml)
docker info --format '{{.ServerVersion}}' >/dev/null
cleanup() {
    local status=$?
    if (( status != 0 )); then
        "${compose[@]}" logs --no-color --tail 80 pd tikv tidb >&2 || true
    fi
    "${compose[@]}" down --volumes --remove-orphans || {
        if (( status == 0 )); then
            status=1
        fi
    }
    printf 'TiDB %s tests finished in %ss (exit %s)\n' "$mode" "$SECONDS" "$status"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up --detach tidb
"${compose[@]}" run --rm --no-deps -T bootstrap
endpoint=$("${compose[@]}" port tidb 4000)
if [[ ! $endpoint =~ ^127\.0\.0\.1:[0-9]+$ ]]; then
    echo "Expected a loopback TiDB port for $project, got $endpoint" >&2
    exit 1
fi
export IAM_TIDB_TEST_URL="mysql://root@$endpoint/iam_test"
export IAM_TIDB_ALLOW_TEST_WRITES=1

if [[ $mode == smoke ]]; then
    "${gateway[@]}" -- --ignored --exact --test-threads=1 "${cases[@]}"
else
    cargo test --locked -p iam-store --features profile tidb_ -- \
        --ignored --skip perf:: --skip tidb_reliability_mixed_load --test-threads=1
    "${gateway[@]}" -- --ignored --skip capacity:: --test-threads=1
fi
