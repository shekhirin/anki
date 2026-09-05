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
    api_key: Option<String>,
    metrics: MetricsState,
}

#[derive(Default, Deserialize)]
struct FileConfig {
    host: Option<String>,
    port: Option<u16>,
    api_key: Option<String>,
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
    api_key: Option<String>,
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
# Require this key on AnkiConnect requests when set. ANKI_API_KEY overrides it.
# api_key = "change-me"
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
            api_key: env_string("ANKI_API_KEY").or(file.api_key),
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

#[derive(Clone, Copy)]
enum SyncOperation {
    Normal,
    Full { upload: bool },
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
#[serde(rename_all = "snake_case")]
enum SyncMode {
    Normal,
    FullDownload,
    FullUpload,
}

#[derive(Deserialize)]
struct SyncRequest {
    mode: SyncMode,
    confirmation: Option<String>,
}

#[derive(Serialize)]
struct SyncAcceptedResponse {
    accepted: bool,
    mode: &'static str,
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
struct AnkiConnectRequest {
    action: String,
    #[serde(default = "default_anki_connect_version")]
    version: u8,
    #[serde(default)]
    params: serde_json::Value,
    key: Option<String>,
}

#[derive(Deserialize)]
struct AnkiConnectMediaInput {
    filename: String,
    #[serde(default)]
    fields: Vec<String>,
    data: Option<String>,
    path: Option<String>,
    url: Option<String>,
    #[serde(rename = "skipHash", default)]
    _skip_hash: bool,
    #[serde(default)]
    _front: bool,
    #[serde(default)]
    _back: bool,
}

#[derive(Deserialize)]
struct AnkiConnectNoteInput {
    #[serde(rename = "deckName")]
    deck_name: String,
    #[serde(rename = "modelName")]
    model_name: String,
    fields: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    audio: Vec<AnkiConnectMediaInput>,
    #[serde(default)]
    video: Vec<AnkiConnectMediaInput>,
    #[serde(default)]
    picture: Vec<AnkiConnectMediaInput>,
}

fn media_data(media: &AnkiConnectMediaInput) -> Result<Vec<u8>, String> {
    if let Some(data) = &media.data {
        data_encoding::BASE64
            .decode(data.as_bytes())
            .map_err(|error| format!("invalid base64 media data: {error}"))
    } else if let Some(path) = &media.path {
        fs::read(path).map_err(|error| format!("unable to read media path {path}: {error}"))
    } else if media.url.is_some() {
        Err("media URLs are not supported; send base64 data or a local path".into())
    } else {
        Err("media requires one of data, path, or url".into())
    }
}

fn append_anki_connect_media(
    backend: &Backend,
    fields: &mut serde_json::Map<String, serde_json::Value>,
    media: AnkiConnectMediaInput,
    image: bool,
) -> Result<(), String> {
    let data = media_data(&media)?;
    let requested_filename = media.filename;
    let filename = backend
        .api_store_media(requested_filename, data)
        .map_err(|error| error.to_string())?;
    let markup = if image {
        format!("<img src=\"{filename}\">")
    } else {
        format!("[sound:{filename}]")
    };
    for field in media.fields {
        append_anki_connect_markup(fields, &field, &markup)?;
    }
    Ok(())
}

fn append_anki_connect_markup(
    fields: &mut serde_json::Map<String, serde_json::Value>,
    field: &str,
    markup: &str,
) -> Result<(), String> {
    let value = fields
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("media field does not exist: {field}"))?;
    fields.insert(
        field.to_owned(),
        serde_json::Value::String(format!("{value}{markup}")),
    );
    Ok(())
}

fn add_anki_connect_note(
    backend: &Backend,
    mut input: AnkiConnectNoteInput,
) -> Result<i64, String> {
    let deck_id = backend
        .api_deck_id_by_name(input.deck_name)
        .map_err(|error| error.to_string())?;
    let notetype_id = backend
        .api_notetype_id_by_name(input.model_name)
        .map_err(|error| error.to_string())?;
    let notetype = backend
        .api_notetype(notetype_id)
        .map_err(|error| error.to_string())?;

    for media in input.audio {
        append_anki_connect_media(&backend, &mut input.fields, media, false)?;
    }
    for media in input.video {
        append_anki_connect_media(&backend, &mut input.fields, media, false)?;
    }
    for media in input.picture {
        append_anki_connect_media(&backend, &mut input.fields, media, true)?;
    }

    let fields = notetype
        .fields
        .into_iter()
        .map(|field| {
            input
                .fields
                .get(&field.name)
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    backend
        .api_add_note(deck_id, notetype_id, fields, input.tags)
        .map_err(|error| error.to_string())
}

fn default_anki_connect_version() -> u8 {
    4
}

const ANKI_CONNECT_ACTIONS: &[&str] = &[
    "version",
    "requestPermission",
    "apiReflect",
    "sync",
    "multi",
    "findCards",
    "findNotes",
    "notesInfo",
    "cardsInfo",
    "cardsToNotes",
    "answerCards",
    "suspend",
    "unsuspend",
    "suspended",
    "areSuspended",
    "getEaseFactors",
    "setEaseFactors",
    "cardsModTime",
    "deckNameFromId",
    "modelNameFromId",
    "modelFieldNames",
    "modelNames",
    "modelNamesAndIds",
    "createDeck",
    "deleteDecks",
    "getDecks",
    "changeDeck",
    "deckNames",
    "deckNamesAndIds",
    "addNote",
    "addNotes",
    "canAddNote",
    "canAddNotes",
    "canAddNotesWithErrorDetail",
    "deleteNotes",
    "getNoteTags",
    "updateNoteFields",
    "updateNoteTags",
    "addTags",
    "removeTags",
    "replaceTags",
    "getTags",
    "storeMediaFile",
    "retrieveMediaFile",
    "getMediaFilesNames",
    "deleteMediaFile",
    "getMediaDirPath",
    "guiBrowse",
    "guiEditNote",
];

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
    let state = Arc::new(AppState {
        backend,
        sync,
        api_key: config.api_key,
        metrics: metrics_state,
    });
    let app = build_app(state);

    let listener = TcpListener::bind((config.host.as_str(), config.port)).await?;
    let address = listener.local_addr()?;
    tracing::info!(%address, "Anki API server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_app(state: Arc<AppState>) -> Router {
    let request_metrics = state.metrics.clone();
    Router::new()
        .route("/health", get(health))
        .route("/", post(ankiconnect))
        .route("/v1/sync", post(sync))
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
        .layer(TraceLayer::new_for_http())
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

    fn trigger(&self, reason: &'static str) -> bool {
        self.trigger_operation(reason, SyncOperation::Normal)
    }

    fn trigger_full(&self, reason: &'static str, upload: bool) -> bool {
        self.trigger_operation(reason, SyncOperation::Full { upload })
    }

    fn trigger_operation(&self, reason: &'static str, operation: SyncOperation) -> bool {
        if self.runtime.in_progress.swap(true, Ordering::AcqRel) {
            tracing::debug!(%reason, "skipping sync because another sync is active");
            return false;
        }

        let controller = self.clone();
        tokio::task::spawn_blocking(move || {
            let result = match operation {
                SyncOperation::Normal => controller
                    .backend
                    .api_sync_collection(controller.auth.clone(), controller.sync_media)
                    .map(|response| format!("completed (required={})", response.required))
                    .map_err(|error| error.to_string()),
                SyncOperation::Full { upload } => controller
                    .backend
                    .api_full_upload_or_download(controller.auth.clone(), upload, None)
                    .map(|_| {
                        if upload {
                            "completed (full_upload)".to_string()
                        } else {
                            "completed (full_download)".to_string()
                        }
                    })
                    .map_err(|error| error.to_string()),
            };
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
        true
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

async fn sync(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SyncRequest>,
) -> impl IntoResponse {
    let Some(controller) = &state.sync else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "sync is not configured".into());
    };

    let (mode, accepted) = match request.mode {
        SyncMode::Normal => ("normal", controller.trigger("api-rest")),
        SyncMode::FullDownload => {
            if request.confirmation.as_deref() != Some("FULL_DOWNLOAD") {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "full download requires confirmation=FULL_DOWNLOAD".into(),
                );
            }
            ("full_download", controller.trigger_full("api-rest", false))
        }
        SyncMode::FullUpload => {
            if request.confirmation.as_deref() != Some("FULL_UPLOAD") {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "full upload requires confirmation=FULL_UPLOAD".into(),
                );
            }
            ("full_upload", controller.trigger_full("api-rest", true))
        }
    };

    if !accepted {
        return error_response(StatusCode::CONFLICT, "a sync is already in progress".into());
    }

    (
        StatusCode::ACCEPTED,
        Json(SyncAcceptedResponse {
            accepted: true,
            mode,
        }),
    )
        .into_response()
}

async fn ankiconnect(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AnkiConnectRequest>,
) -> Json<serde_json::Value> {
    Json(handle_anki_connect_request(state, request).await)
}

async fn handle_anki_connect_request(
    state: Arc<AppState>,
    request: AnkiConnectRequest,
) -> serde_json::Value {
    let legacy_version = request.version <= 4;
    let result = execute_anki_connect_request(state, request).await;

    match result {
        Ok(result) if legacy_version => result,
        Ok(result) => serde_json::json!({"result": result}),
        Err(error) => serde_json::json!({"result": null, "error": error}),
    }
}

fn can_add_note_with_error(
    backend: &Backend,
    candidate: &serde_json::Value,
) -> (bool, Option<String>) {
    let Some(deck_name) = candidate
        .get("deckName")
        .and_then(serde_json::Value::as_str)
    else {
        return (false, Some("deckName is required".into()));
    };
    let Some(model_name) = candidate
        .get("modelName")
        .and_then(serde_json::Value::as_str)
    else {
        return (false, Some("modelName is required".into()));
    };
    let Some(fields) = candidate
        .get("fields")
        .and_then(serde_json::Value::as_object)
    else {
        return (false, Some("fields is required".into()));
    };
    if backend.api_deck_id_by_name(deck_name.to_owned()).is_err() {
        return (false, Some(format!("deck not found: {deck_name}")));
    }
    let model_id = match backend.api_notetype_id_by_name(model_name.to_owned()) {
        Ok(id) => id,
        Err(_) => return (false, Some(format!("model not found: {model_name}"))),
    };
    let model = match backend.api_notetype(model_id) {
        Ok(model) => model,
        Err(error) => return (false, Some(error.to_string())),
    };
    let Some(first_field) = model.fields.first() else {
        return (false, Some("model has no fields".into()));
    };
    let Some(value) = fields
        .get(&first_field.name)
        .and_then(serde_json::Value::as_str)
    else {
        return (
            false,
            Some(format!("field is required: {}", first_field.name)),
        );
    };
    if value.is_empty() {
        return (false, Some("first field is empty".into()));
    }
    let allow_duplicate = candidate
        .get("options")
        .and_then(|options| options.get("allowDuplicate"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if allow_duplicate {
        return (true, None);
    }
    let query = format!("{}:\"{}\"", first_field.name, value.replace('"', ""));
    match backend.api_search_notes(query) {
        Ok(ids) if ids.is_empty() => (true, None),
        Ok(_) => (
            false,
            Some("cannot create note because it is a duplicate".into()),
        ),
        Err(error) => (false, Some(error.to_string())),
    }
}

async fn execute_anki_connect_request(
    state: Arc<AppState>,
    request: AnkiConnectRequest,
) -> Result<serde_json::Value, String> {
    let _version = request.version;
    if let Some(expected_key) = &state.api_key {
        if request.action != "requestPermission"
            && request.key.as_deref() != Some(expected_key.as_str())
        {
            return Err("invalid API key".into());
        }
    }

    let result = match request.action.as_str() {
        "version" => Ok(serde_json::json!(6)),
        "apiReflect" => {
            let scopes = request
                .params
                .get("scopes")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "scopes is required".to_string())?;
            let requested_actions = request.params.get("actions");
            let mut response = serde_json::Map::new();
            let mut returned_scopes = vec![];
            if scopes.iter().any(|scope| scope.as_str() == Some("actions")) {
                let actions = match requested_actions.and_then(serde_json::Value::as_array) {
                    Some(actions) => actions
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .filter(|action| ANKI_CONNECT_ACTIONS.contains(action))
                        .map(str::to_owned)
                        .collect::<Vec<_>>(),
                    None => ANKI_CONNECT_ACTIONS
                        .iter()
                        .map(|action| (*action).into())
                        .collect(),
                };
                returned_scopes.push(serde_json::json!("actions"));
                response.insert("actions".into(), serde_json::json!(actions));
            }
            response.insert("scopes".into(), serde_json::json!(returned_scopes));
            Ok(serde_json::Value::Object(response))
        }
        "requestPermission" => {
            if let Some(expected_key) = &state.api_key {
                if request.key.as_deref() != Some(expected_key.as_str()) {
                    return Ok(serde_json::json!({
                        "permission": "denied",
                        "requireApikey": true,
                        "version": 6,
                    }));
                }
            }
            Ok(serde_json::json!({
                "permission": "granted",
                "requireApikey": state.api_key.is_some(),
                "version": 6,
            }))
        }
        "sync" => match &state.sync {
            Some(sync) => {
                sync.trigger("api");
                Ok(serde_json::Value::Null)
            }
            None => Err("sync is not configured".into()),
        },
        "findCards" => {
            let query = request
                .params
                .get("query")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "query is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            let ids = tokio::task::spawn_blocking(move || backend.api_search_cards(query))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            serde_json::to_value(ids).map_err(|error| error.to_string())
        }
        "findNotes" => {
            let query = request
                .params
                .get("query")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "query is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            let ids = tokio::task::spawn_blocking(move || backend.api_search_notes(query))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            serde_json::to_value(ids).map_err(|error| error.to_string())
        }
        "guiBrowse" => {
            let query = request
                .params
                .get("query")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "query is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            let ids = tokio::task::spawn_blocking(move || backend.api_search_cards(query))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(ids))
        }
        "guiEditNote" => Err("GUI unavailable in headless mode".into()),
        "getTags" => {
            let backend = state.backend.clone();
            let tags = tokio::task::spawn_blocking(move || backend.api_all_tags())
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(tags))
        }
        "storeMediaFile" => {
            let filename = request
                .params
                .get("filename")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "filename is required".to_string())?
                .to_owned();
            let delete_existing = request
                .params
                .get("deleteExisting")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let data = if let Some(encoded) = request
                .params
                .get("data")
                .and_then(serde_json::Value::as_str)
            {
                data_encoding::BASE64
                    .decode(encoded.as_bytes())
                    .map_err(|error| error.to_string())?
            } else if let Some(path) = request
                .params
                .get("path")
                .and_then(serde_json::Value::as_str)
            {
                std::fs::read(path).map_err(|error| error.to_string())?
            } else {
                return Err("one of data or path is required".into());
            };
            let backend = state.backend.clone();
            let stored_name = tokio::task::spawn_blocking(move || {
                if delete_existing {
                    let _ = backend.api_delete_media(filename.clone());
                }
                backend.api_store_media(filename, data)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(stored_name))
        }
        "retrieveMediaFile" => {
            let filename = request
                .params
                .get("filename")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "filename is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            let data = tokio::task::spawn_blocking(move || backend.api_retrieve_media(filename))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(match data {
                Some(data) => serde_json::json!(data_encoding::BASE64.encode(&data)),
                None => serde_json::json!(false),
            })
        }
        "getMediaFilesNames" => {
            let pattern = request
                .params
                .get("pattern")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let backend = state.backend.clone();
            let files = tokio::task::spawn_blocking(move || backend.api_media_files())
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            let files = files
                .into_iter()
                .filter(|file| match pattern.as_deref() {
                    Some(pattern) => file.contains(pattern),
                    None => true,
                })
                .collect::<Vec<_>>();
            Ok(serde_json::json!(files))
        }
        "deleteMediaFile" => {
            let filename = request
                .params
                .get("filename")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "filename is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || backend.api_delete_media(filename))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "getMediaDirPath" => {
            let backend = state.backend.clone();
            let path = tokio::task::spawn_blocking(move || backend.api_media_path(String::new()))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(path))
        }
        "canAddNotesWithErrorDetail" => {
            let candidates = request
                .params
                .get("notes")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "notes is required".to_string())?
                .clone();
            let backend = state.backend.clone();
            let results = tokio::task::spawn_blocking(move || {
                candidates
                    .iter()
                    .map(|candidate| {
                        let (can_add, error) = can_add_note_with_error(&backend, candidate);
                        serde_json::json!({ "canAdd": can_add, "error": error })
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(results))
        }
        "canAddNote" | "canAddNotes" => {
            let candidates = if request.action == "canAddNote" {
                vec![request
                    .params
                    .get("note")
                    .cloned()
                    .ok_or_else(|| "note is required".to_string())?]
            } else {
                request
                    .params
                    .get("notes")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| "notes is required".to_string())?
                    .clone()
            };
            let backend = state.backend.clone();
            let can_add = tokio::task::spawn_blocking(move || {
                candidates
                    .into_iter()
                    .map(|candidate| {
                        let deck_name = candidate
                            .get("deckName")
                            .and_then(serde_json::Value::as_str);
                        let model_name = candidate
                            .get("modelName")
                            .and_then(serde_json::Value::as_str);
                        let fields = candidate
                            .get("fields")
                            .and_then(serde_json::Value::as_object);
                        let allow_duplicate = candidate
                            .get("options")
                            .and_then(|options| options.get("allowDuplicate"))
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        let (Some(deck_name), Some(model_name), Some(fields)) =
                            (deck_name, model_name, fields)
                        else {
                            return false;
                        };
                        if backend.api_deck_id_by_name(deck_name.to_owned()).is_err() {
                            return false;
                        }
                        let model_id = match backend.api_notetype_id_by_name(model_name.to_owned())
                        {
                            Ok(id) => id,
                            Err(_) => return false,
                        };
                        let model = match backend.api_notetype(model_id) {
                            Ok(model) => model,
                            Err(_) => return false,
                        };
                        let Some(first_field) = model.fields.first() else {
                            return false;
                        };
                        let Some(value) = fields
                            .get(&first_field.name)
                            .and_then(serde_json::Value::as_str)
                        else {
                            return false;
                        };
                        if value.is_empty() || allow_duplicate {
                            return !value.is_empty();
                        }
                        let escaped = value.replace('"', "");
                        let query = format!("{}:\"{}\"", first_field.name, escaped);
                        backend
                            .api_search_notes(query)
                            .map(|ids| ids.is_empty())
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|error| error.to_string())?;
            if request.action == "canAddNote" {
                Ok(serde_json::json!(can_add.first().copied().unwrap_or(false)))
            } else {
                Ok(serde_json::json!(can_add))
            }
        }
        "notesInfo" => {
            let note_ids = request
                .params
                .get("notes")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "notes is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "note ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            let notes =
                tokio::task::spawn_blocking(move || {
                    note_ids
                        .into_iter()
                        .map(|id| {
                            let note = backend.api_get_note(id)?;
                            let notetype = backend.api_notetype(note.notetype_id)?;
                            let fields = notetype.fields.into_iter().zip(note.fields).map(
                                |(field, value)| {
                                    let order = field.ord.map(|ord| ord.val).unwrap_or(0);
                                    (
                                        field.name,
                                        serde_json::json!({ "value": value, "order": order }),
                                    )
                                },
                            );
                            let cards = backend.api_cards_of_note(note.id)?;
                            Ok::<_, anki::error::AnkiError>(serde_json::json!({
                                "noteId": note.id,
                                "modelName": notetype.name,
                                "tags": note.tags,
                                "fields": fields.collect::<serde_json::Map<_, _>>(),
                                "cards": cards,
                            }))
                        })
                        .collect::<anki::error::Result<Vec<_>>>()
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(notes))
        }
        "cardsToNotes" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            let notes = tokio::task::spawn_blocking(move || {
                card_ids
                    .into_iter()
                    .map(|id| backend.api_get_card(id).map(|card| card.note_id))
                    .collect::<anki::error::Result<Vec<_>>>()
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(notes))
        }
        "cardsInfo" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            let cards = tokio::task::spawn_blocking(move || {
                card_ids
                    .into_iter()
                    .map(|card_id| {
                        let card = backend.api_get_card(card_id)?;
                        let deck = backend.api_deck(card.deck_id)?;
                        let note = backend.api_get_note(card.note_id)?;
                        let notetype = backend.api_notetype(note.notetype_id)?;
                        Ok::<_, anki::error::AnkiError>(serde_json::json!({
                            "cardId": card.id,
                            // AnkiConnect exposes the related note as its numeric ID.
                            // Keep the card-level metadata below; Yomitan consumes
                            // `note` specifically as an integer.
                            "note": note.id,
                            "deckName": deck.name,
                            "modelName": notetype.name,
                            "ord": card.template_idx,
                            "queue": card.queue,
                            "type": card.ctype,
                            "due": card.due,
                            "interval": card.interval,
                            "factor": card.ease_factor,
                            "reps": card.reps,
                            "lapses": card.lapses,
                            "left": card.remaining_steps,
                            "mod": card.mtime_secs,
                        }))
                    })
                    .collect::<anki::error::Result<Vec<_>>>()
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(cards))
        }
        "getEaseFactors" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            let factors = tokio::task::spawn_blocking(move || {
                card_ids
                    .into_iter()
                    .map(|card_id| {
                        backend
                            .api_get_card(card_id)
                            .map(|card| card.ease_factor as f64 / 1000.0)
                    })
                    .collect::<anki::error::Result<Vec<_>>>()
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(factors))
        }
        "setEaseFactors" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let factors = request
                .params
                .get("factors")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "factors is required".to_string())?
                .iter()
                .map(|factor| {
                    factor
                        .as_f64()
                        .and_then(|factor| {
                            let scaled = (factor * 1000.0).round();
                            (scaled.is_finite() && scaled >= 0.0 && scaled <= u32::MAX as f64)
                                .then_some(scaled as u32)
                        })
                        .ok_or_else(|| "factors must be numbers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            if card_ids.len() != factors.len() {
                return Err("cards and factors must have the same length".into());
            }
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                for (card_id, factor) in card_ids.into_iter().zip(factors) {
                    let mut card = backend.api_get_card(card_id)?;
                    card.ease_factor = factor;
                    backend.api_update_card(card)?;
                }
                Ok::<_, anki::error::AnkiError>(())
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "cardsModTime" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            let times = tokio::task::spawn_blocking(move || {
                card_ids
                    .into_iter()
                    .map(|card_id| backend.api_get_card(card_id).map(|card| card.mtime_secs))
                    .collect::<anki::error::Result<Vec<_>>>()
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(times))
        }
        "deckNameFromId" => {
            let deck_id = request
                .params
                .get("deck")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "deck is required".to_string())?;
            let backend = state.backend.clone();
            let deck = tokio::task::spawn_blocking(move || backend.api_deck(deck_id))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(deck.name))
        }
        "modelNameFromId" => {
            let model_id = request
                .params
                .get("modelId")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "modelId is required".to_string())?;
            let backend = state.backend.clone();
            let model = tokio::task::spawn_blocking(move || backend.api_notetype(model_id))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(model.name))
        }
        "modelFieldNames" => {
            let model_name = request
                .params
                .get("modelName")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "modelName is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            let names = tokio::task::spawn_blocking(move || {
                let model_id = backend.api_notetype_id_by_name(model_name)?;
                backend.api_notetype(model_id).map(|model| {
                    model
                        .fields
                        .into_iter()
                        .map(|field| field.name)
                        .collect::<Vec<_>>()
                })
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(names))
        }
        "answerCards" => {
            let answers = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|card| {
                    let card_id = card
                        .get("cardId")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| "cardId is required".to_string())?;
                    let ease = card
                        .get("ease")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| "ease is required".to_string())?;
                    let ease = i32::try_from(ease)
                        .map_err(|_| "ease must be between 1 and 4".to_string())?;
                    Ok((card_id, ease))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                for (card_id, ease) in answers {
                    backend.api_answer_card(card_id, ease)?;
                }
                Ok::<_, anki::error::AnkiError>(())
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "suspend" | "unsuspend" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let suspend = request.action == "suspend";
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                for card_id in card_ids {
                    let mut card = backend.api_get_card(card_id)?;
                    if suspend {
                        if card.queue != -1 {
                            card.queue = -1;
                            backend.api_update_card(card)?;
                        }
                    } else if card.queue == -1 {
                        card.queue = card.ctype as i32;
                        backend.api_update_card(card)?;
                    }
                }
                Ok::<_, anki::error::AnkiError>(())
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(true))
        }
        "suspended" => {
            let card_id = request
                .params
                .get("card")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "card is required".to_string())?;
            let backend = state.backend.clone();
            let card = tokio::task::spawn_blocking(move || backend.api_get_card(card_id))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(card.queue == -1))
        }
        "areSuspended" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            let suspended = tokio::task::spawn_blocking(move || {
                card_ids
                    .into_iter()
                    .map(|card_id| backend.api_get_card(card_id).map(|card| card.queue == -1))
                    .collect::<anki::error::Result<Vec<_>>>()
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(suspended))
        }
        "addNote" => {
            let input: AnkiConnectNoteInput = serde_json::from_value(
                request
                    .params
                    .get("note")
                    .cloned()
                    .ok_or_else(|| "note is required".to_string())?,
            )
            .map_err(|error| error.to_string())?;
            let backend = state.backend.clone();
            let note_id =
                tokio::task::spawn_blocking(move || add_anki_connect_note(&backend, input))
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(note_id))
        }
        "addNotes" => {
            let inputs = request
                .params
                .get("notes")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "notes is required".to_string())?
                .iter()
                .cloned()
                .map(serde_json::from_value::<AnkiConnectNoteInput>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?;
            let backend = state.backend.clone();
            let note_ids = tokio::task::spawn_blocking(move || {
                inputs
                    .into_iter()
                    .map(|input| add_anki_connect_note(&backend, input).ok())
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(note_ids))
        }
        "deleteNotes" => {
            let note_ids = request
                .params
                .get("notes")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "notes is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "note ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || backend.api_remove_notes(note_ids))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "getNoteTags" => {
            let note_id = request
                .params
                .get("note")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "note is required".to_string())?;
            let backend = state.backend.clone();
            let note = tokio::task::spawn_blocking(move || backend.api_get_note(note_id))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(note.tags))
        }
        "updateNoteFields" => {
            let note = request
                .params
                .get("note")
                .ok_or_else(|| "note is required".to_string())?;
            let note_id = note
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "note.id is required".to_string())?;
            let fields = note
                .get("fields")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| "note.fields is required".to_string())?
                .clone();
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                let mut stored = backend.api_get_note(note_id)?;
                let notetype = backend.api_notetype(stored.notetype_id)?;
                stored.fields = notetype
                    .fields
                    .into_iter()
                    .map(|field| {
                        fields
                            .get(&field.name)
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_owned()
                    })
                    .collect();
                backend.api_update_note(stored)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "updateNoteTags" => {
            let note = request
                .params
                .get("note")
                .ok_or_else(|| "note is required".to_string())?;
            let note_id = note
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "note.id is required".to_string())?;
            let tags = note
                .get("tags")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "note.tags is required".to_string())?
                .iter()
                .map(|tag| {
                    tag.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| "tags must be strings".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                let mut stored = backend.api_get_note(note_id)?;
                stored.tags = tags;
                backend.api_update_note(stored)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "addTags" | "removeTags" | "replaceTags" => {
            let note_ids = request
                .params
                .get("notes")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "notes is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "note ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let tags = request
                .params
                .get("tags")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "tags is required".to_string())?
                .split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let action = request.action.clone();
            let old_tag = request
                .params
                .get("tag")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let replacement = request
                .params
                .get("replaceWith")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                for note_id in note_ids {
                    let mut note = backend.api_get_note(note_id)?;
                    match action.as_str() {
                        "addTags" => {
                            for tag in &tags {
                                if !note.tags.iter().any(|existing| existing == tag) {
                                    note.tags.push(tag.clone());
                                }
                            }
                        }
                        "removeTags" => note
                            .tags
                            .retain(|tag| !tags.iter().any(|remove| remove == tag)),
                        "replaceTags" => {
                            let old_tag = old_tag.as_deref().ok_or_else(|| {
                                anki::error::AnkiError::InvalidInput {
                                    source: anki::error::InvalidInputError {
                                        message: "tag is required".into(),
                                        source: None,
                                        backtrace: None,
                                    },
                                }
                            })?;
                            let replacement = replacement.as_deref().ok_or_else(|| {
                                anki::error::AnkiError::InvalidInput {
                                    source: anki::error::InvalidInputError {
                                        message: "replaceWith is required".into(),
                                        source: None,
                                        backtrace: None,
                                    },
                                }
                            })?;
                            for tag in &mut note.tags {
                                if tag == old_tag {
                                    *tag = replacement.to_owned();
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                    backend.api_update_note(note)?;
                }
                Ok::<_, anki::error::AnkiError>(())
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "createDeck" => {
            let deck_name = request
                .params
                .get("deck")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "deck is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            let deck_id = tokio::task::spawn_blocking(move || backend.api_create_deck(deck_name))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            Ok(serde_json::json!(deck_id))
        }
        "deleteDecks" => {
            let deck_names = request
                .params
                .get("decks")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "decks is required".to_string())?
                .iter()
                .map(|name| {
                    name.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| "deck names must be strings".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                let mut deck_ids = Vec::with_capacity(deck_names.len());
                for name in deck_names {
                    deck_ids.push(backend.api_deck_id_by_name(name)?);
                }
                backend.api_remove_decks(deck_ids)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "getDecks" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let backend = state.backend.clone();
            let decks = tokio::task::spawn_blocking(move || {
                let mut decks = serde_json::Map::new();
                for card_id in card_ids {
                    let card = backend.api_get_card(card_id)?;
                    let deck = backend.api_deck(card.deck_id)?;
                    decks
                        .entry(deck.name)
                        .or_insert_with(|| serde_json::json!([]))
                        .as_array_mut()
                        .expect("deck card list is an array")
                        .push(serde_json::json!(card.id));
                }
                Ok::<_, anki::error::AnkiError>(serde_json::Value::Object(decks))
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(decks)
        }
        "changeDeck" => {
            let card_ids = request
                .params
                .get("cards")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "cards is required".to_string())?
                .iter()
                .map(|id| {
                    id.as_i64()
                        .ok_or_else(|| "card ids must be integers".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let deck_name = request
                .params
                .get("deck")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "deck is required".to_string())?
                .to_owned();
            let backend = state.backend.clone();
            tokio::task::spawn_blocking(move || {
                let deck_id = backend.api_deck_id_by_name(deck_name)?;
                backend.api_set_card_deck(card_ids, deck_id)
            })
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
            Ok(serde_json::Value::Null)
        }
        "deckNames" | "deckNamesAndIds" => {
            let backend = state.backend.clone();
            let decks = tokio::task::spawn_blocking(move || backend.all_decks_json())
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            let decks: serde_json::Map<String, serde_json::Value> =
                serde_json::from_slice(&decks.json).map_err(|error| error.to_string())?;
            if request.action == "deckNames" {
                Ok(serde_json::json!(decks.keys().collect::<Vec<_>>()))
            } else {
                Ok(serde_json::json!(decks
                    .into_iter()
                    .filter_map(|(name, deck)| { deck.get("id").cloned().map(|id| (name, id)) })
                    .collect::<serde_json::Map<_, _>>()))
            }
        }
        "modelNames" | "modelNamesAndIds" => {
            let backend = state.backend.clone();
            let names = tokio::task::spawn_blocking(move || backend.api_notetype_names())
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            if request.action == "modelNames" {
                Ok(serde_json::json!(names
                    .into_iter()
                    .map(|(_, name)| name)
                    .collect::<Vec<_>>()))
            } else {
                Ok(serde_json::json!(names
                    .into_iter()
                    .map(|(id, name)| (name, serde_json::json!(id)))
                    .collect::<serde_json::Map<_, _>>()))
            }
        }
        "multi" => {
            let actions = request
                .params
                .get("actions")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "actions is required".to_string())?;
            let mut responses = Vec::with_capacity(actions.len());
            for action in actions {
                let nested: AnkiConnectRequest =
                    serde_json::from_value(action.clone()).map_err(|error| error.to_string())?;
                responses.push(
                    match Box::pin(execute_anki_connect_request(state.clone(), nested)).await {
                        Ok(result) => result,
                        Err(error) => serde_json::json!({
                            "result": null,
                            "error": error,
                        }),
                    },
                );
            }
            Ok(serde_json::to_value(responses).map_err(|error| error.to_string())?)
        }
        _ => Err(format!("unsupported action: {}", request.action)),
    };

    result
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_inline_media() {
        let media = AnkiConnectMediaInput {
            filename: "sound.mp3".into(),
            fields: vec!["Back".into()],
            data: Some(data_encoding::BASE64.encode(b"audio")),
            path: None,
            url: None,
            _skip_hash: false,
            _front: false,
            _back: false,
        };

        assert_eq!(media_data(&media).unwrap(), b"audio");
    }

    #[test]
    fn reads_media_from_path_and_rejects_urls() {
        let path = std::env::temp_dir().join(format!("anki-api-test-media-{}", std::process::id()));
        fs::write(&path, b"image").unwrap();
        let media = AnkiConnectMediaInput {
            filename: "image.jpg".into(),
            fields: vec!["Back".into()],
            data: None,
            path: Some(path.to_string_lossy().into_owned()),
            url: None,
            _skip_hash: false,
            _front: false,
            _back: false,
        };
        assert_eq!(media_data(&media).unwrap(), b"image");
        fs::remove_file(path).unwrap();

        let media = AnkiConnectMediaInput {
            filename: "remote.mp3".into(),
            fields: vec!["Back".into()],
            data: None,
            path: None,
            url: Some("https://example.com/remote.mp3".into()),
            _skip_hash: false,
            _front: false,
            _back: false,
        };
        assert!(media_data(&media)
            .unwrap_err()
            .contains("URLs are not supported"));
    }

    #[test]
    fn appends_media_markup_to_existing_field() {
        let mut fields = serde_json::Map::from_iter([(
            "Back".into(),
            serde_json::Value::String("definition".into()),
        )]);

        append_anki_connect_markup(&mut fields, "Back", "[sound:word.mp3]").unwrap();
        append_anki_connect_markup(&mut fields, "Back", "<img src=\"word.jpg\">").unwrap();

        assert_eq!(
            fields["Back"],
            serde_json::Value::String("definition[sound:word.mp3]<img src=\"word.jpg\">".into())
        );
    }

    #[test]
    fn parses_ankiconnect_media_fields() {
        let input: AnkiConnectNoteInput = serde_json::from_value(serde_json::json!({
            "deckName": "Default",
            "modelName": "Basic",
            "fields": {"Front": "word", "Back": "definition"},
            "audio": [{
                "filename": "word.mp3",
                "fields": ["Back"],
                "data": "YXVkaW8="
            }],
            "picture": [{
                "filename": "word.jpg",
                "fields": ["Back"],
                "data": "aW1hZ2U="
            }]
        }))
        .unwrap();

        assert_eq!(input.audio.len(), 1);
        assert_eq!(input.picture.len(), 1);
        assert!(input.video.is_empty());
    }

    #[tokio::test]
    async fn ankiconnect_add_note_with_media_over_http() {
        let backend = init_backend(
            &BackendInit {
                preferred_langs: vec!["en".into()],
                server: false,
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .unwrap();
        let metrics = MetricsState {
            start_time: Instant::now(),
            requests_total: Arc::new(AtomicU64::new(0)),
            requests_failed: Arc::new(AtomicU64::new(0)),
            request_duration_nanos: Arc::new(AtomicU64::new(0)),
        };
        let state = Arc::new(AppState {
            backend,
            sync: None,
            api_key: None,
            metrics,
        });
        let app = build_app(state);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let base_url = format!("http://{address}");
        let tempdir = tempfile::tempdir().unwrap();
        let collection_path = tempdir.path().join("collection.anki2");

        let response: serde_json::Value = client
            .post(&base_url)
            .json(&serde_json::json!({
                "action": "version",
                "version": 2,
                "params": {}
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response, serde_json::json!(6));

        let response: serde_json::Value = client
            .post(&base_url)
            .json(&serde_json::json!({
                "action": "version",
                "version": 6,
                "params": {}
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response, serde_json::json!({"result": 6}));

        let response = client
            .post(format!("{base_url}/v1/collection/open"))
            .json(&serde_json::json!({"collection_path": collection_path}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

        let response = client
            .post(&base_url)
            .json(&serde_json::json!({
                "action": "addNote",
                "version": 6,
                "params": {
                    "note": {
                        "deckName": "Default",
                        "modelName": "Basic",
                        "fields": {"Front": "word", "Back": "definition"},
                        "audio": [{
                            "filename": "word.mp3",
                            "fields": ["Back"],
                            "data": "YXVkaW8="
                        }],
                        "picture": [{
                            "filename": "word.jpg",
                            "fields": ["Back"],
                            "data": "aW1hZ2U="
                        }]
                    }
                }
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let response: serde_json::Value = response.json().await.unwrap();
        assert!(!response.as_object().unwrap().contains_key("error"));
        let note_id = response["result"].as_i64().unwrap();

        let response: serde_json::Value = client
            .post(&base_url)
            .json(&serde_json::json!({
                "action": "notesInfo",
                "version": 6,
                "params": {"notes": [note_id]}
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let back = response["result"][0]["fields"]["Back"]["value"]
            .as_str()
            .unwrap();
        assert!(back.contains("[sound:word.mp3]"));
        assert!(back.contains("<img src=\"word.jpg\">"));

        let response: serde_json::Value = client
            .post(&base_url)
            .json(&serde_json::json!({
                "action": "retrieveMediaFile",
                "version": 6,
                "params": {"filename": "word.mp3"}
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["result"], "YXVkaW8=");

        server.abort();
    }
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
