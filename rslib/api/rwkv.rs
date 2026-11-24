// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use std::collections::BTreeMap;
use std::fs;
use std::path::Path as FsPath;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;

use super::*;

mod schedule;
pub(super) use schedule::Settings;

#[derive(Clone)]
pub(super) struct Controller {
    backend: Backend,
    sync: Option<SyncController>,
    gate: Arc<tokio::sync::RwLock<()>>,
    python: String,
    model: PathBuf,
    cache_path: Option<PathBuf>,
    timeout: Duration,
    schedule: Option<schedule::Schedule>,
    runtime: Arc<Runtime>,
}

#[derive(Default)]
struct Runtime {
    running: AtomicBool,
    next_id: AtomicU64,
    jobs: Mutex<BTreeMap<String, Job>>,
    timer: Mutex<schedule::TimerState>,
}

#[derive(Clone, Serialize)]
struct Job {
    job_id: String,
    state: &'static str,
    phase: &'static str,
    applied_cards: usize,
    report: Option<serde_json::Value>,
    error: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunRequest {
    #[serde(default)]
    query: String,
    #[serde(default)]
    apply: bool,
    #[serde(default = "default_sync")]
    sync: bool,
    #[serde(default = "default_retention")]
    retention: f64,
    #[serde(default = "default_maximum_interval")]
    maximum_interval: u32,
}

fn default_sync() -> bool {
    true
}
fn default_retention() -> f64 {
    0.9
}
fn default_maximum_interval() -> u32 {
    3650
}

impl RunRequest {
    fn validate(&self) -> Result<(), String> {
        if !self.retention.is_finite()
            || !(0.0..1.0).contains(&self.retention)
            || self.retention == 0.0
            || !(1..=10_000).contains(&self.maximum_interval)
        {
            return Err("invalid retention or maximum interval".into());
        }
        Ok(())
    }
}

impl Controller {
    pub(super) fn from_config(
        backend: &Backend,
        sync: Option<SyncController>,
        gate: Arc<tokio::sync::RwLock<()>>,
        config: &Config,
    ) -> Result<Option<Self>, String> {
        let schedule = schedule::Schedule::new(&config.rwkv)?;
        if schedule.is_some() && sync.is_none() {
            return Err("nightly RWKV requires AnkiWeb sync credentials".into());
        }
        match (&config.rwkv_python, &config.rwkv_model) {
            (None, None) if schedule.is_none() => Ok(None),
            (Some(python), Some(model)) => {
                if !model.is_file() || config.rwkv_timeout_secs == 0 {
                    return Err(
                        "RWKV requires an existing model file and a positive timeout".into(),
                    );
                }
                Ok(Some(Self {
                    backend: backend.clone(),
                    sync,
                    gate,
                    python: if FsPath::new(python).is_file() {
                        // Preserve a venv's interpreter symlink: resolving it
                        // would drop that environment's installed packages.
                        std::path::absolute(python)
                            .map_err(|error| error.to_string())?
                            .to_string_lossy()
                            .into_owned()
                    } else {
                        python.clone()
                    },
                    model: fs::canonicalize(model).map_err(|error| error.to_string())?,
                    cache_path: config.rwkv_cache_path.clone().or_else(|| {
                        config
                            .collection_path
                            .as_ref()
                            .map(|path| path.with_extension("rwkv-cache.pt"))
                    }),
                    timeout: Duration::from_secs(config.rwkv_timeout_secs),
                    schedule,
                    runtime: Arc::new(Runtime::default()),
                }))
            }
            _ => Err("configure both rwkv_python and rwkv_model to enable RWKV jobs".into()),
        }
    }

    fn start(&self, request: RunRequest) -> Result<String, (StatusCode, String)> {
        request
            .validate()
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        if request.sync && self.sync.is_none() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "sync is not configured".into(),
            ));
        }
        if self.runtime.running.swap(true, Ordering::AcqRel) {
            return Err((StatusCode::CONFLICT, "an RWKV job is already active".into()));
        }
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let id = format!(
            "{:020}-{epoch}-{}",
            self.runtime.next_id.fetch_add(1, Ordering::Relaxed),
            std::process::id()
        );
        {
            let mut jobs = self.runtime.jobs.lock().unwrap();
            while jobs.len() >= 20 {
                jobs.pop_first();
            }
            jobs.insert(
                id.clone(),
                Job {
                    job_id: id.clone(),
                    state: "queued",
                    phase: "waiting_for_collection",
                    applied_cards: 0,
                    report: None,
                    error: None,
                },
            );
        }
        tracing::info!(job_id = %id, apply = request.apply, sync = request.sync, "RWKV job accepted");
        let worker = self.clone();
        let job_id = id.clone();
        tokio::spawn(async move {
            // Includes API requests and automatic sync. No HTTP client owns
            // this guard; disconnecting the cron client cannot release it.
            let _permit = worker.gate.clone().write_owned().await;
            let task = worker.clone();
            let task_id = job_id.clone();
            let result = tokio::task::spawn_blocking(move || task.execute(&task_id, request))
                .await
                .unwrap_or_else(|error| Err(format!("RWKV task failed: {error}")));
            match &result {
                Ok(()) => tracing::info!(job_id = %job_id, "RWKV job succeeded"),
                Err(error) => tracing::error!(job_id = %job_id, %error, "RWKV job failed"),
            }
            worker.update(&job_id, |job| {
                job.state = if result.is_ok() {
                    "succeeded"
                } else {
                    "failed"
                };
                job.phase = "finished";
                job.error = result.err();
            });
            worker.runtime.running.store(false, Ordering::Release);
        });
        Ok(id)
    }

    fn update(&self, id: &str, update: impl FnOnce(&mut Job)) {
        let mut jobs = self.runtime.jobs.lock().unwrap();
        update(jobs.get_mut(id).unwrap());
    }

    fn phase(&self, id: &str, phase: &'static str) {
        tracing::info!(job_id = %id, phase, "RWKV job progress");
        self.update(id, |job| {
            job.state = "running";
            job.phase = phase;
        });
    }

    fn normal_sync(&self) -> Result<(), String> {
        let sync = self.sync.as_ref().ok_or("sync is not configured")?;
        // Collection completion is the scheduling barrier. Media transfer is
        // left to the existing media sync policy and is not needed for replay.
        let result = sync
            .backend
            .api_sync_collection(sync.current_auth(), false)
            .map_err(backend_error)
            .and_then(|response| {
                sync.update_endpoint(response.new_endpoint.clone());
                require_sync_complete(response.required)
            });
        sync.runtime.total.fetch_add(1, Ordering::Relaxed);
        if result.is_err() {
            sync.runtime.failures.fetch_add(1, Ordering::Relaxed);
        } else {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or_default();
            sync.runtime
                .last_success
                .store(timestamp, Ordering::Relaxed);
        }
        *sync.runtime.last_result.lock().unwrap() = Some(match &result {
            Ok(()) => "completed (RWKV collection sync)".into(),
            Err(error) => format!("failed (RWKV): {error}"),
        });
        result
    }

    fn execute(&self, id: &str, request: RunRequest) -> Result<(), String> {
        self.execute_with_sync(id, request, || self.normal_sync())
    }

    fn execute_with_sync(
        &self,
        id: &str,
        request: RunRequest,
        mut synchronize: impl FnMut() -> Result<(), String>,
    ) -> Result<(), String> {
        if request.sync {
            self.phase(id, "sync_before");
            synchronize()?;
        }
        self.phase(id, "snapshot");
        let snapshot = self
            .backend
            .api_rwkv_snapshot(request.query)
            .map_err(backend_error)?;
        self.phase(id, "inference");
        let mut report = self.infer(&snapshot, request.retention, request.maximum_interval)?;
        if request.apply {
            if request.sync {
                self.phase(id, "sync_recheck");
                // Phone reviews may have reached the remote collection while
                // inference ran. Pull again; the apply fingerprint will reject
                // predictions based on any superseded collection state.
                synchronize()?;
            }
            self.phase(id, "apply");
            let proposals = report
                .get("cards")
                .cloned()
                .ok_or("RWKV report has no cards")?;
            let applied = self
                .backend
                .api_rwkv_apply(snapshot, proposals)
                .map_err(backend_error)?;
            report["dry_run"] = serde_json::json!(false);
            self.update(id, |job| {
                job.applied_cards = applied;
                job.report = Some(report.clone());
            });
            if request.sync {
                self.phase(id, "sync_after");
                // On failure the committed changes remain local. Status keeps
                // applied_cards and the report, so callers know to retry sync.
                synchronize()?;
            }
        }
        self.update(id, |job| job.report = Some(report));
        Ok(())
    }

    fn infer(
        &self,
        snapshot: &serde_json::Value,
        retention: f64,
        maximum: u32,
    ) -> Result<serde_json::Value, String> {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        install_worker(directory.path())?;
        let input = directory.path().join("snapshot.json");
        let output = directory.path().join("report.json");
        let stderr_path = directory.path().join("stderr.log");
        fs::write(
            &input,
            serde_json::to_vec(snapshot).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        let stderr = fs::File::create(&stderr_path).map_err(|error| error.to_string())?;
        let mut command = Command::new(&self.python);
        command.args(["-m", "rwkv.preview"]);
        if let Some(path) = &self.cache_path {
            command
                .arg("--cache")
                .arg(std::path::absolute(path).map_err(|error| error.to_string())?);
        }
        let mut child = command
            .current_dir(directory.path())
            .env("PYTHONPATH", directory.path())
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .arg("--model")
            .arg(&self.model)
            .arg("--snapshot")
            .arg(input)
            .arg("--output")
            .arg(&output)
            .arg("--retention")
            .arg(retention.to_string())
            .arg("--maximum-interval")
            .arg(maximum.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .map_err(|error| format!("unable to start RWKV Python: {error}"))?;
        let started = Instant::now();
        loop {
            let status = match child.try_wait() {
                Ok(status) => status,
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("unable to inspect RWKV worker: {error}"));
                }
            };
            if let Some(status) = status {
                if !status.success() {
                    let log = fs::read_to_string(stderr_path).unwrap_or_default();
                    let tail: String = log
                        .chars()
                        .rev()
                        .take(8000)
                        .collect::<String>()
                        .chars()
                        .rev()
                        .collect();
                    return Err(format!("RWKV inference failed: {tail}"));
                }
                break;
            }
            if started.elapsed() >= self.timeout {
                let _ = child.kill();
                let _ = child.wait();
                return Err("RWKV inference timed out; no scheduling changes were applied".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        serde_json::from_slice(&fs::read(output).map_err(|error| error.to_string())?)
            .map_err(|error| format!("invalid RWKV report: {error}"))
    }
}

fn install_worker(root: &FsPath) -> Result<(), String> {
    const FILES: &[(&str, &str)] = &[
        ("__init__.py", include_str!("../../python/rwkv/__init__.py")),
        ("preview.py", include_str!("../../python/rwkv/preview.py")),
        ("cache.py", include_str!("../../python/rwkv/cache.py")),
        (
            "inference/id_rng.py",
            include_str!("../../python/rwkv/inference/id_rng.py"),
        ),
        (
            "inference/small.py",
            include_str!("../../python/rwkv/inference/small.py"),
        ),
        (
            "inference/__init__.py",
            include_str!("../../python/rwkv/inference/__init__.py"),
        ),
        (
            "inference/architecture.py",
            include_str!("../../python/rwkv/inference/architecture.py"),
        ),
        (
            "inference/config.py",
            include_str!("../../python/rwkv/inference/config.py"),
        ),
        (
            "inference/features.py",
            include_str!("../../python/rwkv/inference/features.py"),
        ),
        (
            "inference/ops.py",
            include_str!("../../python/rwkv/inference/ops.py"),
        ),
        (
            "inference/process.py",
            include_str!("../../python/rwkv/inference/process.py"),
        ),
        (
            "inference/rwkv_model.py",
            include_str!("../../python/rwkv/inference/rwkv_model.py"),
        ),
        (
            "inference/rwkv_rnn_model.py",
            include_str!("../../python/rwkv/inference/rwkv_rnn_model.py"),
        ),
        (
            "inference/srs_model_rnn.py",
            include_str!("../../python/rwkv/inference/srs_model_rnn.py"),
        ),
    ];
    fs::create_dir_all(root.join("rwkv/inference")).map_err(|error| error.to_string())?;
    for (name, contents) in FILES {
        fs::write(root.join("rwkv").join(name), contents).map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(super) fn authenticated(state: &AppState, headers: &HeaderMap) -> bool {
    state.api_key.as_ref().map_or(true, |key| {
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            == Some(format!("Bearer {key}").as_str())
    })
}

pub(super) async fn run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<RunRequest>,
) -> axum::response::Response {
    if !authenticated(&state, &headers) {
        return error_response(StatusCode::UNAUTHORIZED, "invalid API key".into());
    }
    let Some(worker) = &state.rwkv else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "RWKV is not configured".into(),
        );
    };
    match worker.start(request) {
        Ok(id) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"job_id": id})),
        )
            .into_response(),
        Err((status, error)) => error_response(status, error),
    }
}

pub(super) async fn status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> axum::response::Response {
    if !authenticated(&state, &headers) {
        return error_response(StatusCode::UNAUTHORIZED, "invalid API key".into());
    }
    let job = state
        .rwkv
        .as_ref()
        .and_then(|worker| worker.runtime.jobs.lock().unwrap().get(&id).cloned());
    match job {
        Some(job) => Json(job).into_response(),
        None => error_response(
            StatusCode::NOT_FOUND,
            "RWKV job not found (jobs are lost on restart)".into(),
        ),
    }
}

pub(super) async fn schedule_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> axum::response::Response {
    if !authenticated(&state, &headers) {
        return error_response(StatusCode::UNAUTHORIZED, "invalid API key".into());
    }
    let Some(worker) = &state.rwkv else {
        return Json(serde_json::json!({"configured": false, "running": false})).into_response();
    };
    let timer = worker.runtime.timer.lock().unwrap().clone();
    let last_job = timer.last_job_id.as_ref().and_then(|id| {
        worker.runtime.jobs.lock().unwrap().get(id).map(|job| {
            serde_json::json!({
                "job_id": job.job_id, "state": job.state, "phase": job.phase,
                "applied_cards": job.applied_cards, "error": job.error,
            })
        })
    });
    Json(serde_json::json!({
        "configured": true,
        "running": worker.runtime.running.load(Ordering::Acquire),
        "schedule": worker.schedule.as_ref().map(|schedule| &schedule.settings),
        "timer": timer,
        "last_job": last_job,
    }))
    .into_response()
}

pub(super) fn backend_error(error: anki::error::AnkiError) -> String {
    match error {
        anki::error::AnkiError::InvalidInput { source } => source.message,
        other => other.to_string(),
    }
}

fn require_sync_complete(required: i32) -> Result<(), String> {
    if required == 0 {
        Ok(())
    } else {
        Err(format!(
            "sync requires further action (required={required}); resolve it before retrying RWKV"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Controller, tempfile::TempDir, i64) {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let python = std::env::var("ANKI_RWKV_TEST_PYTHON").expect("run just test-rwkv");
        let backend = init_backend(&BackendInit::default().encode_to_vec()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        backend
            .open_collection(OpenCollectionRequest {
                collection_path: directory
                    .path()
                    .join("collection.anki2")
                    .to_string_lossy()
                    .into_owned(),
                media_folder_path: directory
                    .path()
                    .join("media")
                    .to_string_lossy()
                    .into_owned(),
                media_db_path: directory
                    .path()
                    .join("media.db")
                    .to_string_lossy()
                    .into_owned(),
            })
            .unwrap();
        let model = backend.api_notetype_id_by_name("Basic".into()).unwrap();
        let note = backend
            .api_add_note(1, model, vec!["question".into(), "answer".into()], vec![])
            .unwrap();
        let card_id = backend.api_cards_of_note(note).unwrap()[0];
        backend.api_answer_card(card_id, 4).unwrap();
        let mut card = backend.api_get_card(card_id).unwrap();
        assert_eq!(card.ctype, 2);
        card.interval = 100;
        card.due = 100;
        backend.api_update_card(card).unwrap();
        let config = Config {
            rwkv_python: Some(python),
            rwkv_model: Some(root.join("out/rwkv/model.bin")),
            rwkv_cache_path: Some(directory.path().join("replay.pt")),
            rwkv_timeout_secs: 30,
            ..Config::default()
        };
        let worker = Controller::from_config(
            &backend,
            None,
            Arc::new(tokio::sync::RwLock::new(())),
            &config,
        )
        .unwrap()
        .unwrap();
        (worker, directory, card_id)
    }

    fn input(apply: bool, sync: bool) -> RunRequest {
        RunRequest {
            query: String::new(),
            apply,
            sync,
            retention: 0.9,
            maximum_interval: 30,
        }
    }

    fn insert_job(worker: &Controller, id: &str) {
        worker.runtime.jobs.lock().unwrap().insert(
            id.into(),
            Job {
                job_id: id.into(),
                state: "queued",
                phase: "waiting_for_collection",
                applied_cards: 0,
                report: None,
                error: None,
            },
        );
    }

    async fn completed(client: &reqwest::Client, base: &str, id: &str) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let job: serde_json::Value = client
                    .get(format!("{base}/v1/rwkv/jobs/{id}"))
                    .bearer_auth("test-key")
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if job["state"] == "succeeded" || job["state"] == "failed" {
                    return job;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "requires the pinned model and Python runtime; run just test-rwkv"]
    async fn rwkv_job_http_applies_once_and_excludes_concurrent_requests() {
        let (worker, _directory, card_id) = fixture();
        let backend = worker.backend.clone();
        let gate = worker.gate.clone();
        let state = Arc::new(AppState {
            backend: backend.clone(),
            sync: None,
            gate: gate.clone(),
            rwkv: Some(worker),
            jiten: None,
            api_key: Some("test-key".into()),
            metrics: MetricsState {
                start_time: Instant::now(),
                requests_total: Arc::new(AtomicU64::new(0)),
                requests_failed: Arc::new(AtomicU64::new(0)),
                request_duration_nanos: Arc::new(AtomicU64::new(0)),
            },
        });
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, build_app(state)).await.unwrap();
        });
        let client = reqwest::Client::new();
        let run_url = format!("{base}/v1/rwkv/run");
        let payload = serde_json::json!({"apply": true, "sync": false, "maximum_interval": 30});
        assert_eq!(
            client
                .post(&run_url)
                .json(&payload)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        // Hold a previous collection operation open, so the job must queue.
        let prior = gate.read().await;
        let accepted = client
            .post(&run_url)
            .bearer_auth("test-key")
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let accepted: serde_json::Value = accepted.json().await.unwrap();
        let id = accepted["job_id"].as_str().unwrap();
        let schedule_status: serde_json::Value = tokio::time::timeout(
            Duration::from_secs(1),
            client
                .get(format!("{base}/v1/rwkv/status"))
                .bearer_auth("test-key")
                .send(),
        )
        .await
        .unwrap()
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(schedule_status["running"], true);
        let duplicate = client
            .post(&run_url)
            .bearer_auth("test-key")
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        let awaiting = client.get(format!("{base}/v1/decks")).send();
        assert!(tokio::time::timeout(Duration::from_millis(30), awaiting)
            .await
            .is_err());
        assert_eq!(backend.api_get_card(card_id).unwrap().interval, 100);
        drop(prior);
        let result = completed(&client, &base, id).await;
        assert_eq!(result["state"], "succeeded", "{result}");
        assert_eq!(result["applied_cards"], 1);
        assert_eq!(result["report"]["dry_run"], false);
        let card = backend.api_get_card(card_id).unwrap();
        assert_eq!(
            u64::from(card.interval),
            result["report"]["cards"][0]["proposed_interval"]
                .as_u64()
                .unwrap()
        );
        let mut repeat_payload = payload.clone();
        repeat_payload["apply"] = serde_json::json!(false);
        let preview: serde_json::Value = client
            .post(&run_url)
            .bearer_auth("test-key")
            .json(&repeat_payload)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let preview = completed(&client, &base, preview["job_id"].as_str().unwrap()).await;
        assert_eq!(preview["state"], "succeeded", "{preview}");
        assert_eq!(preview["applied_cards"], 0);
        assert_eq!(backend.api_get_card(card_id).unwrap(), card);
        let repeat: serde_json::Value = client
            .post(&run_url)
            .bearer_auth("test-key")
            .json(&payload)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let repeat = completed(&client, &base, repeat["job_id"].as_str().unwrap()).await;
        assert_eq!(repeat["state"], "succeeded", "{repeat}");
        assert_eq!(repeat["applied_cards"], 0);
        assert_eq!(repeat["report"]["reviews_replayed"], 0);
        assert_eq!(repeat["report"]["reviews_cached"], 1);
        server.abort();
    }

    #[test]
    #[ignore = "requires the pinned model and Python runtime; run just test-rwkv"]
    fn rwkv_job_sync_failures_stop_before_apply_or_preserve_committed_report() {
        let (worker, _directory, card_id) = fixture();
        insert_job(&worker, "before");
        let original = worker.backend.api_get_card(card_id).unwrap();
        let result = worker.execute_with_sync("before", input(true, true), || {
            Err("initial sync failed".into())
        });
        assert!(result.unwrap_err().contains("initial sync"));
        assert_eq!(worker.backend.api_get_card(card_id).unwrap(), original);
        insert_job(&worker, "after");
        let mut calls = 0;
        let result = worker.execute_with_sync("after", input(true, true), || {
            calls += 1;
            if calls <= 2 {
                Ok(())
            } else {
                Err("upload failed".into())
            }
        });
        assert!(result.unwrap_err().contains("upload failed"));
        assert_eq!(calls, 3);
        let job = worker.runtime.jobs.lock().unwrap()["after"].clone();
        assert_eq!(job.applied_cards, 1);
        assert!(job.report.is_some());
        assert_ne!(worker.backend.api_get_card(card_id).unwrap().interval, 100);
    }

    #[test]
    #[ignore = "requires the pinned model and Python runtime; run just test-rwkv"]
    fn rwkv_job_bad_model_leaves_cards_unchanged() {
        let (mut worker, directory, card_id) = fixture();
        let bad_model = directory.path().join("bad.pth");
        fs::write(&bad_model, "invalid weights").unwrap();
        worker.model = bad_model;
        insert_job(&worker, "bad-model");
        let original = worker.backend.api_get_card(card_id).unwrap();
        let error = worker.execute("bad-model", input(true, false)).unwrap_err();
        assert!(error.contains("checksum mismatch"), "{error}");
        assert_eq!(worker.backend.api_get_card(card_id).unwrap(), original);
    }

    #[test]
    fn rwkv_job_refuses_unresolved_sync_states() {
        assert!(require_sync_complete(0).is_ok());
        for required in 1..=4 {
            assert!(require_sync_complete(required).is_err());
        }
    }

    #[test]
    fn rwkv_schedule_requires_sync_credentials() {
        let backend = init_backend(&BackendInit::default().encode_to_vec()).unwrap();
        let config = Config {
            rwkv: toml::from_str("enabled = true").unwrap(),
            ..Config::default()
        };
        let error = Controller::from_config(
            &backend,
            None,
            Arc::new(tokio::sync::RwLock::new(())),
            &config,
        )
        .err()
        .unwrap();
        assert!(error.contains("sync credentials"));
    }

    #[test]
    #[ignore = "requires the pinned model and Python runtime; run just test-rwkv"]
    fn rwkv_job_recheck_preserves_newer_phone_scheduling() {
        let (worker, _directory, card_id) = fixture();
        insert_job(&worker, "recheck");
        let mut calls = 0;
        let error = worker
            .execute_with_sync("recheck", input(true, true), || {
                calls += 1;
                if calls == 2 {
                    let mut card = worker.backend.api_get_card(card_id).unwrap();
                    card.interval = 99;
                    worker.backend.api_update_card(card).unwrap();
                }
                Ok(())
            })
            .unwrap_err();
        assert!(error.contains("stale"), "{error}");
        assert_eq!(calls, 2);
        assert_eq!(worker.backend.api_get_card(card_id).unwrap().interval, 99);
        assert_eq!(
            worker.runtime.jobs.lock().unwrap()["recheck"].applied_cards,
            0
        );
    }

    #[test]
    #[cfg(unix)]
    fn rwkv_job_timeout_kills_worker_without_writes() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("sleeper");
        fs::write(&executable, "#!/bin/sh\nexec /bin/sleep 5\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let worker = Controller {
            backend: init_backend(&BackendInit::default().encode_to_vec()).unwrap(),
            sync: None,
            gate: Arc::new(tokio::sync::RwLock::new(())),
            python: executable.to_string_lossy().into_owned(),
            model: directory.path().join("unused"),
            cache_path: None,
            timeout: Duration::from_millis(10),
            schedule: None,
            runtime: Arc::new(Runtime::default()),
        };
        let started = Instant::now();
        assert!(worker
            .infer(&serde_json::json!({}), 0.9, 30)
            .unwrap_err()
            .contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
