// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use anki::backend::jiten::Mapping;
use anki::backend::jiten::Snapshot;
use axum::http::HeaderMap;

use super::*;

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Settings {
    enabled: bool,
    auto_sync: bool,
    api_key: Option<String>,
    interval_secs: u64,
    sync_ankiweb_first: bool,
    mappings: Vec<Mapping>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_sync: true,
            api_key: None,
            interval_secs: 3600,
            sync_ankiweb_first: true,
            mappings: vec![],
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
#[serde(default)]
struct Counts {
    imported: u64,
    updated: u64,
    skipped: u64,
    #[serde(rename = "reviewLogs")]
    review_logs: u64,
    #[serde(rename = "skippedWords")]
    skipped_words: Vec<String>,
    #[serde(rename = "skippedWordsReadingMismatch")]
    skipped_words_reading_mismatch: Vec<String>,
}

#[derive(Clone, Default, Serialize)]
struct Status {
    configured: bool,
    running: bool,
    last_success: Option<u64>,
    last_error: Option<String>,
    dry_run: bool,
    selected: usize,
    locally_skipped: usize,
    duplicates: usize,
    counts: Counts,
}

#[derive(Clone)]
pub(super) struct Controller {
    backend: Backend,
    sync: Option<SyncController>,
    gate: Arc<tokio::sync::RwLock<()>>,
    settings: Settings,
    client: reqwest::Client,
    endpoint: String,
    status: Arc<Mutex<Status>>,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct RunRequest {
    dry_run: bool,
}

impl Default for RunRequest {
    fn default() -> Self {
        Self { dry_run: true }
    }
}

impl Controller {
    pub(super) fn new(
        backend: Backend,
        sync: Option<SyncController>,
        gate: Arc<tokio::sync::RwLock<()>>,
        mut settings: Settings,
    ) -> Result<Option<Self>, String> {
        settings.api_key = env_string("ANKI_API_JITEN_API_KEY").or(settings.api_key);
        if !settings.enabled {
            return Ok(None);
        }
        if settings
            .api_key
            .as_deref()
            .map_or(true, |s| s.trim().is_empty())
        {
            return Err("Jiten requires ANKI_API_JITEN_API_KEY or jiten.api_key".into());
        }
        if settings.interval_secs == 0
            || settings.mappings.is_empty()
            || settings
                .mappings
                .iter()
                .any(|m| m.query.trim().is_empty() || m.word_field.trim().is_empty())
        {
            return Err("Jiten requires a positive interval and mappings with nonempty query and word_field".into());
        }
        if settings.sync_ankiweb_first && sync.is_none() {
            return Err(
                "Jiten sync_ankiweb_first requires configured AnkiWeb sync credentials".into(),
            );
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "unable to construct Jiten HTTP client")?;
        Ok(Some(Self {
            backend,
            sync,
            gate,
            settings,
            client,
            endpoint: "https://api.jiten.moe/api/user/vocabulary/import-from-anki".into(),
            status: Arc::new(Mutex::new(Status {
                configured: true,
                ..Default::default()
            })),
        }))
    }

    pub(super) fn start_timer(&self) {
        if !self.settings.auto_sync {
            return;
        }
        let worker = self.clone();
        tokio::spawn(async move {
            let mut timer =
                tokio::time::interval(Duration::from_secs(worker.settings.interval_secs));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Start with one refresh; a restart does not postpone updates indefinitely.
            loop {
                timer.tick().await;
                worker.trigger(false);
            }
        });
    }

    fn trigger(&self, dry_run: bool) -> bool {
        {
            let mut status = self.status.lock().unwrap();
            if status.running {
                return false;
            }
            *status = Status {
                configured: true,
                running: true,
                dry_run,
                last_success: status.last_success,
                ..Default::default()
            };
        }
        let worker = self.clone();
        tokio::spawn(async move {
            let result = worker.execute(dry_run).await;
            let mut status = worker.status.lock().unwrap();
            status.running = false;
            if let Err(error) = result {
                tracing::warn!("Jiten sync failed: {error}");
                status.last_error = Some(error);
            } else if !dry_run {
                status.last_success = Some(now_secs());
                tracing::info!("Jiten sync completed");
            }
        });
        true
    }

    async fn snapshot(&self, dry_run: bool) -> Result<Snapshot, String> {
        // Timer-triggered exports bypass HTTP middleware. Share the operation
        // gate so AnkiWeb sync and capture wait for any exclusive RWKV job.
        let (permit, reservation) = loop {
            let permit = self.gate.clone().read_owned().await;
            let reservation = if let Some(sync) = &self.sync {
                if sync
                    .runtime
                    .in_progress
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    // A timer sync may already have reserved admission while
                    // waiting for the gate. Release it before waiting so a
                    // queued RWKV writer cannot deadlock both readers.
                    drop(permit);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Some(SyncReservation(sync.runtime.clone()))
            } else {
                None
            };
            break (permit, reservation);
        };
        let worker = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // Reserve the same admission flag as timer/manual AnkiWeb syncs.
            // Holding the reservation through capture prevents a sync from replacing the
            // collection.
            let sync = worker.sync.as_ref();
            let _reservation = reservation;
            if worker.settings.sync_ankiweb_first && !dry_run {
                let sync = sync.ok_or("AnkiWeb sync is not configured")?;
                let result = sync
                    .backend
                    .api_sync_collection(sync.current_auth(), sync.sync_media)
                    .map_err(|_| "AnkiWeb sync failed before Jiten export".to_string())?;
                sync.update_endpoint(result.new_endpoint.clone());
                if result.required != 0 {
                    return Err(
                        "AnkiWeb requires further action; resolve it before Jiten export".into(),
                    );
                }
            }
            worker
                .backend
                .api_jiten_snapshot(worker.settings.mappings)
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|_| "Jiten snapshot worker failed".to_string())?
    }

    async fn execute(&self, dry_run: bool) -> Result<(), String> {
        let snapshot = self.snapshot(dry_run).await?;
        {
            let mut status = self.status.lock().unwrap();
            status.selected = snapshot.cards.len();
            status.locally_skipped = snapshot.skipped;
            status.duplicates = snapshot.duplicates;
        }
        if dry_run {
            return Ok(());
        }
        // The API deduplicates reviews. Retrying a partially completed run is safe;
        // always resend the full selected history to catch old imported reviews and
        // edits.
        for cards in snapshot.cards.chunks(200) {
            // Stay below the ordinary authenticated API limit (300 requests/minute).
            tokio::time::sleep(Duration::from_millis(250)).await;
            let body =
                serde_json::json!({ "cards": cards, "overwrite": true, "parseWords": false });
            let counts = self.upload(&body).await?;
            let mut status = self.status.lock().unwrap();
            status.counts.imported += counts.imported;
            status.counts.updated += counts.updated;
            status.counts.skipped += counts.skipped;
            status.counts.review_logs += counts.review_logs;
            status
                .counts
                .skipped_words
                .extend(counts.skipped_words.into_iter().take(50));
            status.counts.skipped_words.truncate(50);
            status
                .counts
                .skipped_words_reading_mismatch
                .extend(counts.skipped_words_reading_mismatch.into_iter().take(50));
            status.counts.skipped_words_reading_mismatch.truncate(50);
        }
        Ok(())
    }

    async fn upload(&self, body: &serde_json::Value) -> Result<Counts, String> {
        for attempt in 0..4 {
            let response = self
                .client
                .post(&self.endpoint)
                .header("X-Api-Key", self.settings.api_key.as_deref().unwrap())
                .json(body)
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    return response
                        .json()
                        .await
                        .map_err(|_| "invalid Jiten import response".into());
                }
                Ok(response) => {
                    let status = response.status();
                    if !(status.as_u16() == 429 || status.is_server_error()) || attempt == 3 {
                        // Do not include response/request bodies or URLs: they may expose secrets.
                        return Err(format!("Jiten import returned HTTP {}", status.as_u16()));
                    }
                    let delay = response
                        .headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(2u64.pow(attempt + 1));
                    if delay > 60 {
                        return Err("Jiten requested a longer retry delay; retry next run".into());
                    }
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
                Err(_) if attempt == 3 => return Err("Jiten request failed after retries".into()),
                Err(_) => tokio::time::sleep(Duration::from_secs(2u64.pow(attempt + 1))).await,
            }
        }
        unreachable!()
    }
}

struct SyncReservation(Arc<SyncRuntimeState>);
impl Drop for SyncReservation {
    fn drop(&mut self) {
        self.0.in_progress.store(false, Ordering::Release);
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    state.api_key.as_ref().map_or(true, |key| {
        headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == format!("Bearer {key}"))
    })
}

pub(super) async fn run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<RunRequest>,
) -> axum::response::Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(controller) = &state.jiten else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Jiten sync is disabled".into(),
        );
    };
    if !controller.trigger(request.dry_run) {
        return error_response(StatusCode::CONFLICT, "Jiten sync is already running".into());
    }
    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"accepted": true})),
    )
        .into_response()
}

pub(super) async fn status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> axum::response::Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let status = state
        .jiten
        .as_ref()
        .map(|c| c.status.lock().unwrap().clone())
        .unwrap_or_default();
    Json(status).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker() -> Controller {
        Controller::new(
            init_backend(
                &BackendInit {
                    preferred_langs: vec!["en".into()],
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .unwrap(),
            None,
            Arc::new(tokio::sync::RwLock::new(())),
            Settings {
                enabled: true,
                api_key: Some("test-secret".into()),
                sync_ankiweb_first: false,
                mappings: vec![Mapping {
                    query: "deck:Default".into(),
                    word_field: "Front".into(),
                    reading_field: None,
                }],
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn jiten_upload_retries_rate_limits_and_reports_auth_failures() {
        let calls = Arc::new(AtomicU64::new(0));
        let handler_calls = calls.clone();
        let app = Router::new().route("/import", post(move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
            let calls = handler_calls.clone();
            async move {
                assert_eq!(headers["X-Api-Key"], "test-secret");
                assert_eq!(body["overwrite"], true);
                if calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    (StatusCode::TOO_MANY_REQUESTS, [("Retry-After", "0")], "rate limited").into_response()
                } else {
                    Json(serde_json::json!({"imported": 1, "updated": 2, "reviewLogs": 3})).into_response()
                }
            }
        })).route("/unauthorized", post(|| async { StatusCode::UNAUTHORIZED }));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut worker = worker();
        worker.endpoint = format!("http://{address}/import");
        let counts = worker
            .upload(&serde_json::json!({"overwrite": true}))
            .await
            .unwrap();
        assert_eq!(
            (counts.imported, counts.updated, counts.review_logs),
            (1, 2, 3)
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        worker.endpoint = format!("http://{address}/unauthorized");
        assert_eq!(
            worker.upload(&serde_json::json!({})).await.err().unwrap(),
            "Jiten import returned HTTP 401"
        );
        server.abort();
    }

    #[tokio::test]
    async fn jiten_preview_is_read_only_and_endpoints_require_authentication() {
        let mut worker = worker();
        let directory = tempfile::tempdir().unwrap();
        worker
            .backend
            .open_collection(OpenCollectionRequest {
                collection_path: directory
                    .path()
                    .join("collection.anki2")
                    .to_string_lossy()
                    .into(),
                ..Default::default()
            })
            .unwrap();
        let nt = worker
            .backend
            .api_notetype_id_by_name("Basic".into())
            .unwrap();
        worker
            .backend
            .api_add_note(1, nt, vec!["猫".into(), "cat".into()], vec![])
            .unwrap();
        let card_id = worker.backend.api_search_cards("".into()).unwrap()[0];
        worker.backend.api_answer_card(card_id, 3).unwrap();
        let before = worker.backend.api_get_card(card_id).unwrap();
        // A nonempty preview succeeds without contacting this unreachable endpoint.
        worker.endpoint = "http://127.0.0.1:1".into();
        worker.execute(true).await.unwrap();
        assert_eq!(worker.status.lock().unwrap().selected, 1);
        assert_eq!(worker.backend.api_get_card(card_id).unwrap(), before);
        assert_eq!(worker.backend.api_search_cards("".into()).unwrap().len(), 1);
        let state = Arc::new(AppState {
            backend: worker.backend.clone(),
            sync: None,
            gate: worker.gate.clone(),
            api_key: Some("local-secret".into()),
            jiten: Some(worker.clone()),
            rwkv: None,
            metrics: MetricsState {
                start_time: Instant::now(),
                requests_total: Arc::new(AtomicU64::new(0)),
                requests_failed: Arc::new(AtomicU64::new(0)),
                request_duration_nanos: Arc::new(AtomicU64::new(0)),
            },
        });
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, build_app(state)).await.unwrap();
        });
        let client = reqwest::Client::new();
        assert_eq!(
            client
                .get(format!("http://{address}/v1/jiten/status"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        // Job control/status remain available while an RWKV job owns the gate.
        let exclusive = worker.gate.clone().write_owned().await;
        worker.status.lock().unwrap().running = true;
        assert_eq!(
            client
                .post(format!("http://{address}/v1/jiten/sync"))
                .bearer_auth("local-secret")
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        let status: serde_json::Value = client
            .get(format!("http://{address}/v1/jiten/status"))
            .bearer_auth("local-secret")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status["configured"], true);
        drop(exclusive);
        server.abort();
    }

    #[tokio::test]
    async fn jiten_snapshot_respects_sync_admission_and_releases_on_error() {
        let mut worker = worker();
        let runtime = Arc::new(SyncRuntimeState {
            in_progress: AtomicBool::new(true),
            total: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            last_success: AtomicU64::new(0),
            last_result: Mutex::new(None),
        });
        worker.sync = Some(SyncController {
            backend: worker.backend.clone(),
            auth: Arc::new(Mutex::new(SyncAuth::default())),
            sync_media: false,
            after_add: false,
            interval_secs: None,
            runtime: runtime.clone(),
            gate: worker.gate.clone(),
        });
        let mut snapshot = Box::pin(worker.snapshot(true));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut snapshot)
                .await
                .is_err()
        );
        assert!(runtime.in_progress.load(Ordering::Acquire));
        // Waiting for sync must not hold the read gate and block RWKV.
        let exclusive =
            tokio::time::timeout(Duration::from_secs(1), worker.gate.clone().write_owned())
                .await
                .unwrap();
        runtime.in_progress.store(false, Ordering::Release);
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut snapshot)
                .await
                .is_err()
        );
        drop(exclusive);
        // No collection is open, so capture fails after acquiring the reservation.
        assert!(tokio::time::timeout(Duration::from_secs(1), snapshot)
            .await
            .unwrap()
            .is_err());
        assert!(!runtime.in_progress.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn jiten_snapshot_waits_for_exclusive_rwkv_job() {
        let worker = worker();
        let exclusive = worker.gate.clone().write_owned().await;
        let mut snapshot = Box::pin(worker.snapshot(true));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut snapshot)
                .await
                .is_err()
        );
        drop(exclusive);
        // Reaches the backend only after admission; this fixture has no open
        // collection.
        assert!(tokio::time::timeout(Duration::from_secs(1), snapshot)
            .await
            .unwrap()
            .is_err());
    }

    #[tokio::test]
    async fn jiten_timer_uploads_reviewed_vocabulary() {
        let mut worker = worker();
        let directory = tempfile::tempdir().unwrap();
        worker
            .backend
            .open_collection(OpenCollectionRequest {
                collection_path: directory
                    .path()
                    .join("collection.anki2")
                    .to_string_lossy()
                    .into(),
                ..Default::default()
            })
            .unwrap();
        let nt = worker
            .backend
            .api_notetype_id_by_name("Basic".into())
            .unwrap();
        worker
            .backend
            .api_add_note(1, nt, vec!["猫".into(), "cat".into()], vec![])
            .unwrap();
        let id = worker.backend.api_search_cards("".into()).unwrap()[0];
        worker.backend.api_answer_card(id, 3).unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let received = calls.clone();
        let app = Router::new().route(
            "/import",
            post(move |Json(body): Json<serde_json::Value>| {
                let calls = received.clone();
                async move {
                    assert_eq!(body["cards"][0]["card"]["word"], "猫");
                    assert_eq!(body["cards"][0]["reviewLogs"][0]["rating"], 3);
                    assert_eq!(body["parseWords"], false);
                    calls.fetch_add(1, Ordering::Relaxed);
                    Json(serde_json::json!({"imported": 1, "reviewLogs": 1}))
                }
            }),
        );
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        worker.endpoint = format!("http://{}/import", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        worker.start_timer();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if worker.status.lock().unwrap().last_success.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let status = worker.status.lock().unwrap().clone();
        assert!(!status.running);
        assert!(status.last_error.is_none());
        assert_eq!(status.counts.imported, 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        server.abort();
    }
}
