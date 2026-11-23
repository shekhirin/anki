# Nightly RWKV-Curve scheduling

The headless server can reschedule ordinary review cards using RWKV-Curve,
while standard phone apps continue using their supported scheduler during the
day. The nightly worker syncs, replays history, proposes intervals, checks for
newly synced reviews, applies changes, and syncs again. A preview is the default;
`--apply` explicitly enables writes.

## Setup

From the repository root:

```sh
just rwkv-model
just rwkv-setup
```

Add these settings to `~/.anki/server.toml`, using absolute paths:

```toml
collection_path = "/path/to/collection.anki2"
# Existing sync credentials; a timer is not required for manual/RWKV sync.
sync_username = "your-ankiweb-user"
sync_password = "your-password"
# sync_endpoint = "https://sync.ankiweb.net"
rwkv_python = "/path/to/anki/out/rwkv/venv/bin/python"
rwkv_model = "/path/to/anki/out/rwkv/model.pth"
rwkv_timeout_secs = 3600
```

The corresponding environment overrides are `ANKI_API_RWKV_PYTHON`,
`ANKI_API_RWKV_MODEL`, and `ANKI_API_RWKV_TIMEOUT_SECS`. The timeout bounds
inference, not network sync. Runtime and model settings are supplied by the
server administrator; HTTP requests cannot select programs or model paths.

Start or restart the server:

```sh
just api-server
```

Preview the complete coordinated job, then enable writes:

```sh
just rwkv-run --output out/rwkv/preview-job.json
just rwkv-run --apply --output out/rwkv/nightly-job.json
```

Optional arguments include `--query 'deck:Default'`, `--retention 0.9`, and
`--maximum-interval 3650`. Set `ANKI_API_KEY` in the client environment if the
server requires a key. `--url` selects another server. `--no-sync` explicitly
selects local-only operation; synced operation is the default.

`rwkv-setup` installs the pinned Python/PyTorch runtime in `out/rwkv/venv`. The
job client uses that interpreter directly, so cron does not need `uv` on PATH.

## Cron

For example, run daily at 04:15 in the cron host's timezone, after Anki's usual
04:00 rollover. Use the actual absolute paths for the checkout and `just`:

```cron
15 4 * * * cd /path/to/anki && /path/to/just rwkv-run --apply --output /path/to/anki/out/rwkv/nightly-job.json >> /path/to/anki/out/rwkv/nightly.log 2>&1
```

If authentication is enabled, make the same `ANKI_API_KEY` available in the cron
environment. The server must already be running. No crontab is installed by the
setup recipes.

Sync your phone after studying and before the next session. The server locks
its own collection operations, not remote phones: avoid studying during the
nightly job. A second pull immediately before applying catches remote changes
that arrive during inference and rejects the stale preview.

## Coordination and failures

- Jobs return a unique ID immediately and continue if the client disconnects.
  The CLI prints that ID before polling. Use
  `just rwkv-run --job-id ID` to inspect an accepted job without starting another.
- Only one RWKV job is accepted at once. An exclusive operation gate covers the
  complete job, including its syncs. Other API collection requests and automatic
  syncs wait until the gate is released; job admission/status remain available.
- Full-sync requirements, initial sync errors, invalid models, worker failures,
  and inference timeouts stop the job before scheduling changes are made.
- Immediately before commit, the backend verifies a fingerprint of the current
  card state, history, target selection, and scheduler day. This catches older
  reviews arriving late through sync, resets, edits, and rollover. Stale plans
  are rejected instead of overwriting newer scheduling.
- Every proposal is validated before one native transaction writes the batch.
  Changes update Anki's modification/sync metadata and create administrative
  rescheduling history. They do not invent answers. The transaction creates one
  undo entry; a subsequent normal sync can clear Anki's local undo history.
- If the final upload fails, the job fails but retains `applied_cards` and the
  report. Local changes remain committed. Resolve sync and retry; a new run
  starts with sync and recomputes from current history. Unchanged scheduling is
  not rewritten and adds no administrative entries.
- A CLI timeout does not cancel a job. The CLI never automatically resubmits a
  write after losing a response. If the admission response was lost, inspect the
  server log for the accepted `job_id` before starting another job. Completed
  reports are written atomically. Exit status is nonzero on failure.
- The server retains the most recent 20 jobs in memory. Jobs/status IDs are not
  durable across a server restart; the CLI's output file is the saved record.

## HTTP endpoints

All RWKV endpoints use `Authorization: Bearer <key>` when an API key is configured.

- `POST /v1/rwkv/run` accepts `query` (default `""`), `apply` (default `false`),
  `sync` (default `true`), `retention` (default `0.9`), and `maximum_interval`
  (default `3650`). It returns HTTP 202 with `job_id`, or HTTP 409 for an active
  job. Credentials are required when `sync` is true.
- `GET /v1/rwkv/jobs/{job_id}` returns `state` (`queued`, `running`, `succeeded`,
  `failed`), `phase`, `applied_cards`, `report`, and `error`.
- `POST /v1/rwkv/snapshot` accepts `{"query":""}` and exports a read-only,
  immutable snapshot. It does not sync or require a configured RWKV runtime.

## Offline previews

The original read-only preview command remains available:

```sh
just rwkv-preview out/rwkv/model.pth --query 'deck:Default' --retention 0.9 --output out/rwkv/report.json --save-snapshot out/rwkv/snapshot.json
just rwkv-preview out/rwkv/model.pth --snapshot out/rwkv/snapshot.json --output out/rwkv/report.json
```

Snapshots contain review metadata and IDs, without note text or media. Offline
reports cannot be submitted directly as writes: the server job owns snapshot
capture, inference, validation, and apply.

## Scheduling semantics

- Targets are ordinary review cards. New, learning, relearning, suspended,
  buried, and filtered cards are excluded. Phone scheduler settings and its
  ease, counters, FSRS memory state, and actual last-review time are preserved.
- Replay is chronological across the collection, including rated history of
  deleted cards with missing catalog identities. Manual rescheduling and
  non-rescheduling filtered reviews are excluded. A reset discards that card's
  preceding history; currently-new cards' old histories are excluded.
- Historical note/deck/preset membership is not recorded in Anki's revlog;
  replay uses the current catalog and home decks for filtered cards.
- Scheduler-day boundaries use the configured UTC offset and rollover hour,
  falling back to the server's current offset if no offset is configured.
  Historical travel/DST changes cannot be reconstructed from an offset alone.
- Curve output is captured after each card's latest actual answer. The interval
  is the first whole day at or below desired retention, without assuming the
  curve is monotonic. It is capped by `--maximum-interval` (allowed 1–10000).
- Due dates are anchored to the last review's scheduler day and clamped to today
  if overdue. An interval never starts afresh from the nightly run time.
- Reports include unchanged targets with active history. Due-by-tomorrow counts
  cover these targets only, including backlog, not the full collection workload.
- Each run rebuilds state from the complete snapshot. CPU reference inference
  can be slow and memory intensive on large collections. Incremental checkpoints
  and GPU inference are not implemented.

## Provenance

`inference/` is ported from the AGPL-3.0-or-later reference implementation in
[JSchoreels/anki](https://github.com/JSchoreels/anki/tree/afa298cbe077b61fc42d214b2bf5898e300d3c98/qt/aqt/rwkv_inference),
commit `afa298cbe077b61fc42d214b2bf5898e300d3c98`. Original notices are retained.
The spaced-repetition adaptation originates in
[1DWalker's work in srs-benchmark](https://github.com/open-spaced-repetition/srs-benchmark).
This uses PyTorch, not the fork's optimized Rust engine or the separately
licensed RWKV-SRS runtime.

The model is downloaded explicitly from the same commit, outside tracked source.
Its SHA-256 is
`67ee8e9b0d0169733242c40646e0d2e5637845a25775762002ffc4e6396f9590`.
Other artifacts, including Instant-only and incompatible models, are rejected.
This does not claim parity with the fork's newer small/tiny models or desktop
queue behavior. Worker sources are embedded in the server binary, so deployed
servers do not need a separate source checkout for inference.

## Verification

```sh
just test-rwkv
just check
```

The focused recipe includes real-model HTTP jobs, duplicate/concurrent admission,
read-only previews, idempotent reruns, stale-history rejection, atomic validation,
undo, fallback-state preservation, worker failure/timeout, sync failure stages,
and cron-client reporting. Real-model worker tests are explicitly ignored by
the general Rust suite and enabled by `just test-rwkv`.
