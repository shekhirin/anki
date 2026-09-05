// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use std::fs;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use anki::backend::init_backend;
use anki::backend::Backend;
use anki::services::BackendCollectionService;
use anki_proto::backend::BackendInit;
use anki_proto::collection::OpenCollectionRequest;
use anki_proto::sync::SyncAuth;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::Request;
use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::Router;
use prost::Message;
use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct MetricsState {
    start_time: Instant,
    requests_total: Arc<AtomicU64>,
    requests_failed: Arc<AtomicU64>,
    request_duration_nanos: Arc<AtomicU64>,
}

#[derive(Clone)]
struct AppState {
    backend: Backend,
    sync: Option<SyncController>,
    metrics: MetricsState,
}

#[derive(Default, Deserialize)]
struct FileConfig {
    host: Option<String>,
    port: Option<u16>,
    collection_path: Option<String>,
    sync_username: Option<String>,
    sync_password: Option<String>,
    sync_endpoint: Option<String>,
    sync_interval_secs: Option<u64>,
    sync_after_add: Option<bool>,
    sync_media: Option<bool>,
}

#[derive(Clone)]
struct Config {
    host: String,
    port: u16,
    collection_path: Option<PathBuf>,
    sync_username: Option<String>,
    sync_password: Option<String>,
    sync_endpoint: Option<String>,
    sync_interval_secs: Option<u64>,
    sync_after_add: bool,
    sync_media: bool,
}

const DEFAULT_CONFIG: &str = r#"# Anki headless API server configuration.
# Environment variables with the ANKI_API_ prefix override these values.

host = "127.0.0.1"
port = 8765
# Open this collection automatically on startup.
# collection_path = "/path/to/collection.anki2"

# Enable automatic sync by uncommenting these settings.
# sync_username = "your-ankiweb-user"
# sync_password = "your-password"
# sync_endpoint = "https://sync.ankiweb.net"
# sync_interval_secs = 300
# sync_after_add = true
sync_media = true
"#;

impl Config {
    fn load() -> Result<Self, String> {
        let file = if let Some(home) = dirs::home_dir() {
            let path = home.join(".anki/server.toml");
            create_default_config(&path)?;
            let contents = fs::read_to_string(&path)
                .map_err(|error| format!("unable to read {}: {error}", path.display()))?;
            toml::from_str::<FileConfig>(&contents)
                .map_err(|error| format!("unable to parse {}: {error}", path.display()))?
        } else {
            FileConfig::default()
        };

        let host = env_string("ANKI_API_HOST")
            .or(file.host)
            .unwrap_or_else(|| "127.0.0.1".into());
        let port = match env_string("ANKI_API_PORT") {
            Some(value) => value
                .parse()
                .map_err(|_| "ANKI_API_PORT must be an integer".to_string())?,
            None => file.port.unwrap_or(8765),
        };
        let sync_interval_secs = match env_string("ANKI_API_SYNC_INTERVAL_SECS") {
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| "ANKI_API_SYNC_INTERVAL_SECS must be an integer".to_string())?,
            ),
            None => file.sync_interval_secs,
        };

        let collection_path = env_string("ANKI_API_COLLECTION_PATH")
            .or(file.collection_path)
            .map(expand_home_path);

        Ok(Self {
            host,
            port,
            collection_path,
            sync_username: env_string("ANKI_API_SYNC_USERNAME").or(file.sync_username),
            sync_password: env_string("ANKI_API_SYNC_PASSWORD").or(file.sync_password),
            sync_endpoint: env_string("ANKI_API_SYNC_ENDPOINT").or(file.sync_endpoint),
            sync_interval_secs,
            sync_after_add: env_bool(
                "ANKI_API_SYNC_AFTER_ADD",
                file.sync_after_add.unwrap_or(false),
            ),
            sync_media: env_bool("ANKI_API_SYNC_MEDIA", file.sync_media.unwrap_or(true)),
        })
    }
}

fn expand_home_path(path: String) -> PathBuf {
    match (path.as_str(), dirs::home_dir()) {
        ("~", Some(home)) => home,
        (path, Some(home)) if path.starts_with("~/") => home.join(&path[2..]),
        _ => PathBuf::from(path),
    }
}

fn create_default_config(path: &std::path::Path) -> Result<(), String> {
    if path.exists() {
        return Ok(());
    }

    let parent = path.parent().ok_or_else(|| {
        format!(
            "unable to determine parent directory for {}",
            path.display()
        )
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("unable to create {}: {error}", parent.display()))?;

    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => file
            .write_all(DEFAULT_CONFIG.as_bytes())
            .map_err(|error| format!("unable to write {}: {error}", path.display())),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(format!("unable to create {}: {error}", path.display())),
    }
}

#[derive(Clone)]
struct SyncController {
    backend: Backend,
    auth: SyncAuth,
    sync_media: bool,
    after_add: bool,
    interval_secs: Option<u64>,
    runtime: Arc<SyncRuntimeState>,
}

struct SyncRuntimeState {
    in_progress: AtomicBool,
    total: AtomicU64,
    failures: AtomicU64,
    last_success: AtomicU64,
    last_result: Mutex<Option<String>>,
}

#[derive(Serialize)]
struct SyncStatusResponse {
    configured: bool,
    after_add: bool,
    interval_secs: Option<u64>,
    in_progress: bool,
    last_result: Option<String>,
}

#[derive(Deserialize)]
struct OpenRequest {
    collection_path: PathBuf,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    collection_open: bool,
}

#[derive(Deserialize)]
struct SearchQuery {
    query: String,
}

#[derive(Deserialize)]
struct AddNoteRequest {
    deck_id: Option<i64>,
    deck_name: Option<String>,
    notetype_id: Option<i64>,
    model_name: Option<String>,
    fields: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Deserialize)]
struct AnswerRequest {
    rating: i32,
}

#[derive(Serialize)]
struct IdResponse {
    id: i64,
}

#[derive(Serialize)]
struct NoteResponse {
    id: i64,
    notetype_id: i64,
    fields: Vec<String>,
    tags: Vec<String>,
}

#[derive(Serialize)]
struct CardResponse {
    id: i64,
    note_id: i64,
    deck_id: i64,
    queue: i32,
    due: i32,
    interval: u32,
    reps: u32,
    lapses: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    anki::log::set_global_logger(None)?;

    let config = Config::load().map_err(std::io::Error::other)?;

    let backend = init_backend(
        &BackendInit {
            preferred_langs: vec!["en".into()],
            server: false,
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .map_err(std::io::Error::other)?;

    if let Some(collection_path) = &config.collection_path {
        let media_folder_path = collection_path.with_extension("media");
        let media_db_path = collection_path.with_extension("mdb");
        backend
            .open_collection(OpenCollectionRequest {
                collection_path: collection_path.to_string_lossy().into_owned(),
                media_folder_path: media_folder_path.to_string_lossy().into_owned(),
                media_db_path: media_db_path.to_string_lossy().into_owned(),
            })
            .map_err(|error| {
                std::io::Error::other(format!(
                    "unable to open configured collection {}: {error}",
                    collection_path.display()
                ))
            })?;
    }

    // Sync login uses the backend's synchronous wrapper around an async
    // operation. Run it outside Tokio's runtime to avoid nested block_on().
    let sync = tokio::task::spawn_blocking({
        let backend = backend.clone();
        let config = config.clone();
        move || SyncController::from_config(&backend, &config)
    })
    .await
    .map_err(std::io::Error::other)?
    .map_err(std::io::Error::other)?;
    if let Some(controller) = &sync {
        if let Some(interval_secs) = controller.interval_secs {
            start_sync_timer(controller.clone(), interval_secs);
        }
    }

    let metrics_state = MetricsState {
        start_time: Instant::now(),
        requests_total: Arc::new(AtomicU64::new(0)),
        requests_failed: Arc::new(AtomicU64::new(0)),
        request_duration_nanos: Arc::new(AtomicU64::new(0)),
    };
    let request_metrics = metrics_state.clone();
    let state = Arc::new(AppState {
        backend,
        sync,
        metrics: metrics_state,
    });
    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/sync/status", get(sync_status))
        .route("/v1/metrics", get(metrics_endpoint))
        .route("/v1/collection/open", post(open_collection))
        .route("/v1/collection/close", post(close_collection))
        .route("/v1/decks", get(get_decks))
        .route("/v1/cards/search", get(search_cards))
        .route("/v1/cards/{card_id}", get(get_card))
        .route("/v1/cards/{card_id}/answer", post(answer_card))
        .route("/v1/notes/{note_id}", get(get_note))
        .route("/v1/notes", post(add_note))
        .with_state(state)
        .layer(axum::middleware::from_fn(
            move |request: Request, next: Next| {
                let metrics = request_metrics.clone();
                async move {
                    let started = Instant::now();
                    let response = next.run(request).await;
                    let status = response.status();
                    metrics.requests_total.fetch_add(1, Ordering::Relaxed);
                    if status.is_client_error() || status.is_server_error() {
                        metrics.requests_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    metrics
                        .request_duration_nanos
                        .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    response
                }
            },
        ))
        .layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind((config.host.as_str(), config.port)).await?;
    let address = listener.local_addr()?;
    tracing::info!(%address, "Anki API server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

impl SyncController {
    fn from_config(backend: &Backend, config: &Config) -> Result<Option<Self>, String> {
        let username = config.sync_username.clone();
        let password = config.sync_password.clone();
        let after_add = config.sync_after_add;
        let interval_secs = config.sync_interval_secs;

        if let Some(secs) = interval_secs {
            if secs == 0 {
                return Err("ANKI_API_SYNC_INTERVAL_SECS must be greater than zero".into());
            }
        }

        if !after_add && interval_secs.is_none() {
            return Ok(None);
        }

        let username = username.ok_or_else(|| {
            "ANKI_API_SYNC_USERNAME is required when auto-sync is enabled".to_string()
        })?;
        let password = password.ok_or_else(|| {
            "ANKI_API_SYNC_PASSWORD is required when auto-sync is enabled".to_string()
        })?;
        let endpoint = config.sync_endpoint.clone();
        let auth = backend
            .api_sync_login(username, password, endpoint)
            .map_err(|error| format!("sync login failed: {error}"))?;

        Ok(Some(Self {
            backend: backend.clone(),
            auth,
            sync_media: config.sync_media,
            after_add,
            interval_secs,
            runtime: Arc::new(SyncRuntimeState {
                in_progress: AtomicBool::new(false),
                total: AtomicU64::new(0),
                failures: AtomicU64::new(0),
                last_success: AtomicU64::new(0),
                last_result: Mutex::new(None),
            }),
        }))
    }

    fn trigger(&self, reason: &'static str) {
        if self.runtime.in_progress.swap(true, Ordering::AcqRel) {
            tracing::debug!(%reason, "skipping sync because another sync is active");
            return;
        }

        let controller = self.clone();
        tokio::task::spawn_blocking(move || {
            let result = controller
                .backend
                .api_sync_collection(controller.auth.clone(), controller.sync_media)
                .map(|response| format!("completed (required={})", response.required))
                .map_err(|error| error.to_string());
            controller.runtime.total.fetch_add(1, Ordering::Relaxed);
            if result.is_err() {
                controller.runtime.failures.fetch_add(1, Ordering::Relaxed);
            } else {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| duration.as_secs())
                    .unwrap_or_default();
                controller
                    .runtime
                    .last_success
                    .store(timestamp, Ordering::Relaxed);
            }
            let message = match result {
                Ok(message) => message,
                Err(error) => format!("failed: {error}"),
            };
            tracing::info!(%reason, result = %message, "auto-sync finished");
            *controller.runtime.last_result.lock().unwrap() = Some(message);
            controller
                .runtime
                .in_progress
                .store(false, Ordering::Release);
        });
    }

    fn status(&self) -> SyncStatusResponse {
        SyncStatusResponse {
            configured: true,
            after_add: self.after_add,
            interval_secs: self.interval_secs,
            in_progress: self.runtime.in_progress.load(Ordering::Acquire),
            last_result: self.runtime.last_result.lock().unwrap().clone(),
        }
    }
}

fn env_string(name: &str) -> Option<String> {
    std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name).as_deref() {
        Ok("1" | "true" | "yes" | "on") => true,
        Ok("0" | "false" | "no" | "off") => false,
        Ok(_) => default,
        Err(_) => default,
    }
}

fn start_sync_timer(controller: SyncController, interval_secs: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await;
        loop {
            interval.tick().await;
            controller.trigger("timer");
        }
    });
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        collection_open: state.backend.collection_is_open(),
    })
}

async fn sync_status(State(state): State<Arc<AppState>>) -> Json<SyncStatusResponse> {
    match &state.sync {
        Some(sync) => Json(sync.status()),
        None => Json(SyncStatusResponse {
            configured: false,
            after_add: false,
            interval_secs: None,
            in_progress: false,
            last_result: None,
        }),
    }
}

async fn open_collection(
    State(state): State<Arc<AppState>>,
    Json(request): Json<OpenRequest>,
) -> impl IntoResponse {
    let collection_path = request.collection_path;
    let media_folder_path = collection_path.with_extension("media");
    let media_db_path = collection_path.with_extension("mdb");

    let backend = state.backend.clone();
    match tokio::task::spawn_blocking(move || {
        backend.open_collection(OpenCollectionRequest {
            collection_path: collection_path.to_string_lossy().into_owned(),
            media_folder_path: media_folder_path.to_string_lossy().into_owned(),
            media_db_path: media_db_path.to_string_lossy().into_owned(),
        })
    })
    .await
    {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(error)) => error_response(StatusCode::BAD_REQUEST, error.to_string()),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn close_collection(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let backend = state.backend.clone();
    match tokio::task::spawn_blocking(move || {
        backend.close_collection(anki_proto::collection::CloseCollectionRequest {
            downgrade_to_schema11: false,
        })
    })
    .await
    {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(error)) => error_response(StatusCode::BAD_REQUEST, error.to_string()),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn get_decks(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let backend = state.backend.clone();
    match tokio::task::spawn_blocking(move || backend.all_decks_json()).await {
        Ok(result) => match result {
            Ok(decks) => match serde_json::from_slice::<serde_json::Value>(&decks.json) {
                Ok(json) => (StatusCode::OK, Json(json)).into_response(),
                Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
            },
            Err(error) => error_response(StatusCode::BAD_REQUEST, error.to_string()),
        },
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn search_cards(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    let backend = state.backend.clone();
    match tokio::task::spawn_blocking(move || backend.api_search_cards(query.query)).await {
        Ok(Ok(ids)) => Json(ids).into_response(),
        Ok(Err(error)) => error_response(StatusCode::BAD_REQUEST, error.to_string()),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn get_card(
    State(state): State<Arc<AppState>>,
    Path(card_id): Path<i64>,
) -> impl IntoResponse {
    let backend = state.backend.clone();
    match tokio::task::spawn_blocking(move || backend.api_get_card(card_id)).await {
        Ok(Ok(card)) => Json(CardResponse {
            id: card.id,
            note_id: card.note_id,
            deck_id: card.deck_id,
            queue: card.queue,
            due: card.due,
            interval: card.interval,
            reps: card.reps,
            lapses: card.lapses,
        })
        .into_response(),
        Ok(Err(error)) => error_response(StatusCode::NOT_FOUND, error.to_string()),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn get_note(
    State(state): State<Arc<AppState>>,
    Path(note_id): Path<i64>,
) -> impl IntoResponse {
    let backend = state.backend.clone();
    match tokio::task::spawn_blocking(move || backend.api_get_note(note_id)).await {
        Ok(Ok(note)) => Json(NoteResponse {
            id: note.id,
            notetype_id: note.notetype_id,
            fields: note.fields,
            tags: note.tags,
        })
        .into_response(),
        Ok(Err(error)) => error_response(StatusCode::NOT_FOUND, error.to_string()),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn add_note(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AddNoteRequest>,
) -> impl IntoResponse {
    let backend = state.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<i64, String> {
        let deck_id = if let Some(id) = request.deck_id {
            id
        } else if let Some(name) = request.deck_name {
            backend
                .api_deck_id_by_name(name)
                .map_err(|error| error.to_string())?
        } else {
            return Err("deck_id or deck_name is required".into());
        };
        let notetype_id = if let Some(id) = request.notetype_id {
            id
        } else if let Some(name) = request.model_name {
            backend
                .api_notetype_id_by_name(name)
                .map_err(|error| error.to_string())?
        } else {
            return Err("notetype_id or model_name is required".into());
        };
        backend
            .api_add_note(deck_id, notetype_id, request.fields, request.tags)
            .map_err(|error| error.to_string())
    })
    .await;
    match result {
        Ok(Ok(id)) => {
            if let Some(sync) = &state.sync {
                if sync.after_add {
                    sync.trigger("note-added");
                }
            }
            (StatusCode::CREATED, Json(IdResponse { id })).into_response()
        }
        Ok(Err(error)) => error_response(StatusCode::BAD_REQUEST, error),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn answer_card(
    State(state): State<Arc<AppState>>,
    Path(card_id): Path<i64>,
    Json(request): Json<AnswerRequest>,
) -> impl IntoResponse {
    let backend = state.backend.clone();
    match tokio::task::spawn_blocking(move || backend.api_answer_card(card_id, request.rating))
        .await
    {
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(error)) => error_response(StatusCode::BAD_REQUEST, error.to_string()),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

fn error_response(status: StatusCode, message: String) -> axum::response::Response {
    (status, Json(ErrorResponse { error: message })).into_response()
}

async fn metrics_endpoint(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut metrics = String::new();

    let collection_open = state.backend.collection_is_open();

    metrics.push_str("# HELP anki_api_uptime_seconds Server uptime in seconds\n");
    metrics.push_str("# TYPE anki_api_uptime_seconds counter\n");
    metrics.push_str(&format!(
        "anki_api_uptime_seconds {}\n",
        state.metrics.start_time.elapsed().as_secs_f64()
    ));

    metrics.push_str("# HELP anki_http_requests_total Total HTTP requests handled\n");
    metrics.push_str("# TYPE anki_http_requests_total counter\n");
    metrics.push_str(&format!(
        "anki_http_requests_total {}\n",
        state.metrics.requests_total.load(Ordering::Relaxed)
    ));

    metrics.push_str("# HELP anki_http_requests_failed_total HTTP requests returning 4xx or 5xx\n");
    metrics.push_str("# TYPE anki_http_requests_failed_total counter\n");
    metrics.push_str(&format!(
        "anki_http_requests_failed_total {}\n",
        state.metrics.requests_failed.load(Ordering::Relaxed)
    ));

    metrics.push_str("# HELP anki_http_request_duration_seconds_sum Total HTTP request time\n");
    metrics.push_str("# TYPE anki_http_request_duration_seconds_sum counter\n");
    metrics.push_str(&format!(
        "anki_http_request_duration_seconds_sum {}\n",
        state.metrics.request_duration_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0
    ));

    metrics.push_str("# HELP anki_http_request_duration_seconds_count Total HTTP requests timed\n");
    metrics.push_str("# TYPE anki_http_request_duration_seconds_count counter\n");
    metrics.push_str(&format!(
        "anki_http_request_duration_seconds_count {}\n",
        state.metrics.requests_total.load(Ordering::Relaxed)
    ));

    metrics.push_str("# HELP anki_collection_open Whether a collection is currently open\n");
    metrics.push_str("# TYPE anki_collection_open gauge\n");
    metrics.push_str(&format!(
        "anki_collection_open {}\n",
        if collection_open { 1 } else { 0 }
    ));

    if let Some(sync) = &state.sync {
        metrics.push_str("# HELP anki_sync_in_progress Whether a sync is currently running\n");
        metrics.push_str("# TYPE anki_sync_in_progress gauge\n");
        metrics.push_str(&format!(
            "anki_sync_in_progress {}\n",
            sync.runtime.in_progress.load(Ordering::Relaxed) as u8
        ));
        metrics.push_str("# HELP anki_sync_total Completed sync attempts\n");
        metrics.push_str("# TYPE anki_sync_total counter\n");
        metrics.push_str(&format!(
            "anki_sync_total {}\n",
            sync.runtime.total.load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP anki_sync_failures_total Failed sync attempts\n");
        metrics.push_str("# TYPE anki_sync_failures_total counter\n");
        metrics.push_str(&format!(
            "anki_sync_failures_total {}\n",
            sync.runtime.failures.load(Ordering::Relaxed)
        ));
        metrics.push_str("# HELP anki_sync_last_success Unix timestamp of last successful sync\n");
        metrics.push_str("# TYPE anki_sync_last_success gauge\n");
        metrics.push_str(&format!(
            "anki_sync_last_success {}\n",
            sync.runtime.last_success.load(Ordering::Relaxed)
        ));
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        metrics,
    )
}
