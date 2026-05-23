//! Core send/receive logic, decoupled from CLI and GUI.
//!
//! This module contains the pure network logic: importing files, creating
//! tickets, connecting peers, transferring blobs, and exporting received data.
//! Progress is reported through `tokio::sync::mpsc` channels.

use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    net::{SocketAddrV4, SocketAddrV6},
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use data_encoding::HEXLOWER;
use futures_buffered::BufferedStreamExt;
use iroh::{
    address_lookup::{dns::DnsAddressLookup, pkarr::PkarrPublisher},
    endpoint::presets,
    Endpoint, EndpointAddr, RelayMode, RelayUrl, SecretKey, TransportAddr,
};
use iroh_blobs::{
    api::{
        blobs::{
            AddPathOptions, AddProgressItem, ExportMode, ExportOptions, ExportProgressItem,
            ImportMode,
        },
        remote::GetProgressItem,
        Store, TempTag,
    },
    format::collection::Collection,
    get::{request::get_hash_seq_and_sizes, GetError, Stats},
    provider::{
        self,
        events::{ConnectMode, EventMask, EventSender, ProviderMessage, RequestUpdate},
    },
    store::fs::FsStore,
    ticket::BlobTicket,
    BlobFormat, BlobsProtocol, Hash,
};
use n0_future::{task::AbortOnDropHandle, FuturesUnordered, StreamExt};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, trace};
use walkdir::WalkDir;

// ── Public types ──────────────────────────────────────────────────────────

/// Hash display format.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    #[default]
    Hex,
    Cid,
}

impl FromStr for Format {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "hex" => Ok(Format::Hex),
            "cid" => Ok(Format::Cid),
            _ => Err(anyhow::anyhow!("invalid format")),
        }
    }
}

impl Display for Format {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Format::Hex => write!(f, "hex"),
            Format::Cid => write!(f, "cid"),
        }
    }
}

pub fn print_hash(hash: &Hash, format: Format) -> String {
    match format {
        Format::Hex => hash.to_hex().to_string(),
        Format::Cid => hash.to_string(),
    }
}

/// Options to configure what is included in an [`EndpointAddr`].
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Default,
    Debug,
    derive_more::Display,
    derive_more::FromStr,
    Serialize,
    Deserialize,
)]
pub enum AddrInfoOptions {
    #[default]
    Id,
    RelayAndAddresses,
    Relay,
    Addresses,
}

pub fn apply_options(addr: &mut EndpointAddr, opts: AddrInfoOptions) {
    match opts {
        AddrInfoOptions::Id => {
            addr.addrs = Default::default();
        }
        AddrInfoOptions::RelayAndAddresses => {}
        AddrInfoOptions::Relay => {
            addr.addrs = addr
                .addrs
                .iter()
                .filter(|addr| matches!(addr, TransportAddr::Relay(_)))
                .cloned()
                .collect();
        }
        AddrInfoOptions::Addresses => {
            addr.addrs = addr
                .addrs
                .iter()
                .filter(|addr| matches!(addr, TransportAddr::Ip(_)))
                .cloned()
                .collect();
        }
    }
}

/// Relay mode option (mirrors CLI but without clap dependency).
#[derive(Clone, Debug)]
pub enum RelayModeOption {
    Disabled,
    Default,
    Custom(RelayUrl),
}

impl Default for RelayModeOption {
    fn default() -> Self {
        RelayModeOption::Default
    }
}

impl FromStr for RelayModeOption {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "disabled" => Ok(Self::Disabled),
            "default" => Ok(Self::Default),
            _ => Ok(Self::Custom(RelayUrl::from_str(s)?)),
        }
    }
}

impl Display for RelayModeOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => f.write_str("disabled"),
            Self::Default => f.write_str("default"),
            Self::Custom(url) => url.fmt(f),
        }
    }
}

impl From<RelayModeOption> for RelayMode {
    fn from(value: RelayModeOption) -> Self {
        match value {
            RelayModeOption::Disabled => RelayMode::Disabled,
            RelayModeOption::Default => RelayMode::Default,
            RelayModeOption::Custom(url) => RelayMode::Custom(url.into()),
        }
    }
}

// ── Configuration structs ─────────────────────────────────────────────────

/// Configuration for a send operation.
#[derive(Debug, Clone)]
pub struct SendConfig {
    /// Path to the file or directory to send.
    pub path: PathBuf,
    /// What type of ticket to use.
    pub ticket_type: AddrInfoOptions,
    /// Relay mode.
    pub relay_mode: RelayModeOption,
    /// Hash display format (reserved for future use).
    #[allow(dead_code)]
    pub format: Format,
    /// Optional fixed IPv4 address.
    pub magic_ipv4_addr: Option<SocketAddrV4>,
    /// Optional fixed IPv6 address.
    pub magic_ipv6_addr: Option<SocketAddrV6>,
    /// Number of parallel import jobs.
    pub jobs: Option<usize>,
    /// Verbosity level.
    pub verbose: u8,
    /// Print the secret key.
    pub show_secret: bool,
}

impl Default for SendConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            ticket_type: AddrInfoOptions::RelayAndAddresses,
            relay_mode: RelayModeOption::Default,
            format: Format::Hex,
            magic_ipv4_addr: None,
            magic_ipv6_addr: None,
            jobs: None,
            verbose: 0,
            show_secret: false,
        }
    }
}

/// Configuration for a receive operation.
#[derive(Debug, Clone)]
pub struct ReceiveConfig {
    /// The ticket to connect to the sender.
    pub ticket: BlobTicket,
    /// Relay mode.
    pub relay_mode: RelayModeOption,
    /// Hash display format (reserved for future use).
    #[allow(dead_code)]
    pub format: Format,
    /// Optional fixed IPv4 address.
    pub magic_ipv4_addr: Option<SocketAddrV4>,
    /// Optional fixed IPv6 address.
    pub magic_ipv6_addr: Option<SocketAddrV6>,
    /// Verbosity level.
    pub verbose: u8,
}

// ── Progress events ───────────────────────────────────────────────────────

/// Progress updates emitted during a send operation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SendProgress {
    /// Import has begun; total file count known.
    ImportStarted { total_files: u64 },
    /// Progress on the current file being imported.
    ImportFile {
        name: String,
        progress: u64,
        size: u64,
    },
    /// Import of all files complete.
    ImportDone {
        hash: Hash,
        size: u64,
        #[allow(dead_code)]
        elapsed: Duration,
    },
    /// Endpoint is online, waiting for connections.
    WaitingForConnection,
    /// A client connected.
    ClientConnected {
        connection_id: u64,
        remote_id: String,
    },
    /// Transfer progress for a specific request.
    TransferProgress {
        request_id: u64,
        progress: u64,
        total: u64,
    },
    /// A transfer request completed.
    TransferDone { request_id: u64 },
    /// A connection closed.
    ConnectionClosed { connection_id: u64 },
    /// Ticket is ready — the user can share it.
    TicketReady { ticket: String, hash: Hash, size: u64 },
    /// Shutting down the endpoint.
    ShuttingDown,
    /// Send operation finished.
    Done,
}

/// Progress updates emitted during a receive operation.
#[derive(Debug, Clone)]
pub enum ReceiveProgress {
    /// Connecting to the sender.
    Connecting,
    /// Connected, getting file sizes.
    GettingSizes,
    /// Collection info received.
    CollectionInfo {
        total_files: u64,
        total_size: u64,
    },
    /// Download progress.
    Downloading {
        progress: u64,
        total: u64,
    },
    /// Export has begun.
    ExportStarted { total_files: u64 },
    /// Progress on the current file being exported.
    ExportFile {
        name: String,
        progress: u64,
        size: u64,
    },
    /// Export complete.
    Done {
        total_files: u64,
        payload_size: u64,
        elapsed: Duration,
    },
    /// An error occurred.
    Error(String),
}

// ── Helper functions ──────────────────────────────────────────────────────

/// Get the secret key or generate a new one.
pub fn get_or_create_secret(print: bool) -> anyhow::Result<SecretKey> {
    match std::env::var("IROH_SECRET") {
        Ok(secret) => SecretKey::from_str(&secret).context("invalid secret"),
        Err(_) => {
            let key = SecretKey::generate();
            if print {
                let key = hex::encode(key.to_bytes());
                eprintln!("using secret key {key}");
            }
            Ok(key)
        }
    }
}

pub fn validate_path_component(component: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !component.contains('/'),
        "path components must not contain the only correct path separator, /"
    );
    Ok(())
}

/// Convert a canonicalized path to a string.
pub fn canonicalized_path_to_string(
    path: impl AsRef<Path>,
    must_be_relative: bool,
) -> anyhow::Result<String> {
    let mut path_str = String::new();
    let parts = path
        .as_ref()
        .components()
        .filter_map(|c| match c {
            Component::Normal(x) => {
                let c = match x.to_str() {
                    Some(c) => c,
                    None => return Some(Err(anyhow::anyhow!("invalid character in path"))),
                };
                if !c.contains('/') && !c.contains('\\') {
                    Some(Ok(c))
                } else {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                }
            }
            Component::RootDir => {
                if must_be_relative {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                } else {
                    path_str.push('/');
                    None
                }
            }
            _ => Some(Err(anyhow::anyhow!("invalid path component {:?}", c))),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let parts = parts.join("/");
    path_str.push_str(&parts);
    Ok(path_str)
}

pub fn get_export_path(root: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let parts = name.split('/');
    let mut path = root.to_path_buf();
    for part in parts {
        validate_path_component(part)?;
        path.push(part);
    }
    Ok(path)
}

// ── Import / Export (progress-aware) ──────────────────────────────────────

/// Import a file or directory into the database, sending progress events.
async fn import(
    path: PathBuf,
    db: &Store,
    jobs: Option<usize>,
    progress: mpsc::Sender<SendProgress>,
) -> anyhow::Result<(TempTag, u64, Collection)> {
    let parallelism = jobs.unwrap_or_else(num_cpus::get);
    let path = path.canonicalize()?;
    anyhow::ensure!(path.exists(), "path {} does not exist", path.display());
    let root = path.parent().context("context get parent")?;
    let files = WalkDir::new(path.clone()).into_iter();
    let data_sources: Vec<(String, PathBuf)> = files
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type().is_file() {
                return Ok(None);
            }
            let path = entry.into_path();
            let relative = path.strip_prefix(root)?;
            let name = canonicalized_path_to_string(relative, true)?;
            anyhow::Ok(Some((name, path)))
        })
        .filter_map(Result::transpose)
        .collect::<anyhow::Result<Vec<_>>>()?;

    let total = data_sources.len() as u64;
    progress
        .send(SendProgress::ImportStarted {
            total_files: total,
        })
        .await
        .ok();

    let names_and_tags = n0_future::stream::iter(data_sources)
        .map(|(name, path)| {
            let db = db.clone();
            let progress = progress.clone();
            async move {
                let import = db.add_path_with_opts(AddPathOptions {
                    path: path.clone(),
                    mode: ImportMode::TryReference,
                    format: BlobFormat::Raw,
                });
                let mut stream = import.stream().await;
                let mut item_size = 0;
                let temp_tag = loop {
                    let item = stream
                        .next()
                        .await
                        .context("import stream ended without a tag")?;
                    trace!("importing {name} {item:?}");
                    match item {
                        AddProgressItem::Size(size) => {
                            item_size = size;
                            progress
                                .send(SendProgress::ImportFile {
                                    name: name.clone(),
                                    progress: 0,
                                    size,
                                })
                                .await
                                .ok();
                        }
                        AddProgressItem::CopyProgress(offset) => {
                            progress
                                .send(SendProgress::ImportFile {
                                    name: name.clone(),
                                    progress: offset,
                                    size: item_size,
                                })
                                .await
                                .ok();
                        }
                        AddProgressItem::OutboardProgress(offset) => {
                            progress
                                .send(SendProgress::ImportFile {
                                    name: name.clone(),
                                    progress: offset,
                                    size: item_size,
                                })
                                .await
                                .ok();
                        }
                        AddProgressItem::Error(cause) => {
                            anyhow::bail!("error importing {}: {}", name, cause);
                        }
                        AddProgressItem::Done(tt) => break tt,
                        _ => {}
                    }
                };
                anyhow::Ok((name, temp_tag, item_size))
            }
        })
        .buffered_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut names_and_tags = names_and_tags;
    names_and_tags.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    let size = names_and_tags.iter().map(|(_, _, size)| *size).sum::<u64>();
    let (collection, tags) = names_and_tags
        .into_iter()
        .map(|(name, tag, _)| ((name, tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let temp_tag = collection.clone().store(db).await?;
    drop(tags);
    Ok((temp_tag, size, collection))
}

/// Export a collection to the current directory, sending progress events.
async fn export(
    db: &Store,
    collection: Collection,
    progress: mpsc::Sender<ReceiveProgress>,
) -> anyhow::Result<()> {
    let root = std::env::current_dir()?;
    let total = collection.len() as u64;
    progress
        .send(ReceiveProgress::ExportStarted {
            total_files: total,
        })
        .await
        .ok();
    for (name, hash) in collection.iter() {
        let target = get_export_path(&root, name)?;
        if target.exists() {
            anyhow::bail!("target {} already exists", target.display());
        }
        let mut stream = db
            .export_with_opts(ExportOptions {
                hash: *hash,
                target,
                mode: ExportMode::Copy,
            })
            .stream()
            .await;
        let mut file_size = 0;
        while let Some(item) = stream.next().await {
            match item {
                ExportProgressItem::Size(size) => {
                    file_size = size;
                    progress
                        .send(ReceiveProgress::ExportFile {
                            name: name.to_string(),
                            progress: 0,
                            size,
                        })
                        .await
                        .ok();
                }
                ExportProgressItem::CopyProgress(offset) => {
                    progress
                        .send(ReceiveProgress::ExportFile {
                            name: name.to_string(),
                            progress: offset,
                            size: file_size,
                        })
                        .await
                        .ok();
                }
                ExportProgressItem::Error(cause) => {
                    anyhow::bail!("error exporting {}: {}", name, cause);
                }
                ExportProgressItem::Done => {}
            }
        }
    }
    Ok(())
}

// ── Core async functions ──────────────────────────────────────────────────

/// Execute a send operation, reporting progress through the channel.
///
/// Returns once the endpoint is online and the ticket is ready.
/// The returned `SendHandle` keeps the endpoint alive; drop it to shut down.
pub async fn send_files(
    config: SendConfig,
    progress: mpsc::Sender<SendProgress>,
) -> anyhow::Result<SendHandle> {
    let secret_key = get_or_create_secret(config.verbose > 0)?;
    if config.show_secret {
        eprintln!(
            "using secret key {}",
            hex::encode(secret_key.to_bytes())
        );
    }

    let relay_mode: RelayMode = config.relay_mode.into();
    let mut builder = Endpoint::builder(presets::N0)
        .alpns(vec![iroh_blobs::protocol::ALPN.to_vec()])
        .secret_key(secret_key)
        .relay_mode(relay_mode.clone());

    if config.ticket_type == AddrInfoOptions::Id {
        builder = builder.address_lookup(PkarrPublisher::n0_dns());
    }
    if let Some(addr) = config.magic_ipv4_addr {
        builder = builder.bind_addr(addr)?;
    }
    if let Some(addr) = config.magic_ipv6_addr {
        builder = builder.bind_addr(addr)?;
    }

    let suffix = rand::rng().random::<[u8; 16]>();
    let cwd = std::env::current_dir()?;
    let blobs_data_dir = cwd.join(format!(".sendme-send-{}", HEXLOWER.encode(&suffix)));
    if blobs_data_dir.exists() {
        anyhow::bail!(
            "can not share twice from the same directory: {}",
            cwd.display()
        );
    }
    if cwd.join(&config.path) == cwd {
        anyhow::bail!("can not share from the current directory");
    }

    let blobs_data_dir2 = blobs_data_dir.clone();
    let (provider_tx, provider_rx) = mpsc::channel(32);
    let progress_clone = progress.clone();

    tokio::fs::create_dir_all(&blobs_data_dir2).await?;

    let endpoint = builder.bind().await?;
    let store = FsStore::load(&blobs_data_dir2).await?;
    let blobs = BlobsProtocol::new(
        &store,
        Some(EventSender::new(
            provider_tx,
            EventMask {
                connected: ConnectMode::Notify,
                get: provider::events::RequestMode::NotifyLog,
                ..EventMask::DEFAULT
            },
        )),
    );

    let t0 = Instant::now();
    let (temp_tag, size, _collection) =
        import(config.path, blobs.store(), config.jobs, progress.clone()).await?;
    let dt = t0.elapsed();
    let hash = temp_tag.hash();

    progress
        .send(SendProgress::ImportDone {
            hash,
            size,
            elapsed: dt,
        })
        .await
        .ok();

    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    // Wait for the endpoint to figure out its address
    let ep = router.endpoint();
    tokio::time::timeout(Duration::from_secs(30), async move {
        if !matches!(relay_mode, RelayMode::Disabled) {
            let _ = ep.online().await;
        }
    })
    .await?;

    progress
        .send(SendProgress::WaitingForConnection)
        .await
        .ok();

    // Make ticket
    let mut addr = router.endpoint().addr();
    apply_options(&mut addr, config.ticket_type);
    let ticket =
        BlobTicket::new(addr, hash, BlobFormat::HashSeq).to_string();

    progress
        .send(SendProgress::TicketReady {
            ticket: ticket.clone(),
            hash,
            size,
        })
        .await
        .ok();

    // Spawn provider event handler
    let handle = AbortOnDropHandle::new(n0_future::task::spawn(handle_provider_events(
        provider_rx,
        progress_clone,
    )));

    Ok(SendHandle {
        _router: router,
        _temp_tag: temp_tag,
        blobs_data_dir,
        _provider_handle: handle,
        ticket,
        hash,
        size,
    })
}

/// Handle that keeps a send session alive. Drop it to shut down.
#[allow(dead_code)]
pub struct SendHandle {
    _router: iroh::protocol::Router,
    _temp_tag: TempTag,
    blobs_data_dir: PathBuf,
    _provider_handle: AbortOnDropHandle<()>,
    pub ticket: String,
    pub hash: Hash,
    pub size: u64,
}

impl SendHandle {
    /// Shut down the send session gracefully and clean up temporary data.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        // Drop ownership of _router to shut it down
        let SendHandle {
            _router,
            _temp_tag,
            blobs_data_dir,
            _provider_handle,
            ..
        } = self;
        drop(_temp_tag);
        drop(_router);
        // Give the endpoint time to close
        tokio::time::sleep(Duration::from_millis(500)).await;
        if blobs_data_dir.exists() {
            tokio::fs::remove_dir_all(&blobs_data_dir).await?;
        }
        Ok(())
    }
}

/// Handle provider events (connections, requests) and forward progress.
async fn handle_provider_events(
    mut recv: mpsc::Receiver<ProviderMessage>,
    progress: mpsc::Sender<SendProgress>,
) {
    let connections = Arc::new(Mutex::new(BTreeMap::new()));
    let mut tasks = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            item = recv.recv() => {
                let Some(item) = item else { break };
                trace!("got provider event {item:?}");
                match item {
                    ProviderMessage::ClientConnectedNotify(msg) => {
                        let remote_id = msg.endpoint_id
                            .map(|id| id.fmt_short().to_string())
                            .unwrap_or_else(|| "?".to_string());
                        let connection_id = msg.connection_id;
                        connections.lock().unwrap().insert(
                            connection_id,
                            PerConnectionProgress {
                                requests: BTreeMap::new(),
                                _remote_id: remote_id.clone(),
                            },
                        );
                        progress
                            .send(SendProgress::ClientConnected {
                                connection_id,
                                remote_id,
                            })
                            .await
                            .ok();
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        let removed = connections.lock().unwrap().remove(&msg.connection_id).is_some();
                        // MutexGuard dropped above; safe to await now
                        if removed {
                            progress
                                .send(SendProgress::ConnectionClosed {
                                    connection_id: msg.connection_id,
                                })
                                .await
                                .ok();
                        }
                    }
                    ProviderMessage::GetRequestReceivedNotify(msg) => {
                        let request_id = msg.request_id;
                        let connection_id = msg.connection_id;
                        let connections = connections.clone();
                        let progress = progress.clone();
                        tasks.push(per_request_progress(
                            connection_id, request_id, connections, msg.rx, progress,
                        ));
                    }
                    _ => {}
                }
            }
            Some(_) = tasks.next(), if !tasks.is_empty() => {}
        }
    }
    while tasks.next().await.is_some() {}
}

struct PerConnectionProgress {
    _remote_id: String,
    requests: BTreeMap<u64, ()>,
}

async fn per_request_progress(
    connection_id: u64,
    request_id: u64,
    connections: Arc<Mutex<BTreeMap<u64, PerConnectionProgress>>>,
    mut rx: irpc::channel::mpsc::Receiver<RequestUpdate>,
    progress: mpsc::Sender<SendProgress>,
) {
    if let Some(connection) = connections.lock().unwrap().get_mut(&connection_id) {
        connection.requests.insert(request_id, ());
    } else {
        error!("got request for unknown connection {connection_id}");
        return;
    }
    let mut total_size = 0u64;
    while let Ok(Some(msg)) = rx.recv().await {
        match msg {
            RequestUpdate::Started(msg) => {
                total_size = msg.size;
                progress
                    .send(SendProgress::TransferProgress {
                        request_id,
                        progress: 0,
                        total: total_size,
                    })
                    .await
                    .ok();
            }
            RequestUpdate::Progress(msg) => {
                progress
                    .send(SendProgress::TransferProgress {
                        request_id,
                        progress: msg.end_offset,
                        total: total_size,
                    })
                    .await
                    .ok();
            }
            RequestUpdate::Completed(_) | RequestUpdate::Aborted(_) => {
                if let Some(msg) = connections.lock().unwrap().get_mut(&connection_id) {
                    msg.requests.remove(&request_id);
                }
                progress
                    .send(SendProgress::TransferDone { request_id })
                    .await
                    .ok();
                break;
            }
        }
    }
}

fn show_get_error(e: GetError) -> GetError {
    match &e {
        GetError::InitialNext { source, .. } => {
            eprintln!("initial connection error: {source}")
        }
        GetError::ConnectedNext { source, .. } => eprintln!("connected error: {source}"),
        GetError::AtBlobHeaderNext { source, .. } => {
            eprintln!("reading blob header error: {source}")
        }
        GetError::Decode { source, .. } => eprintln!("decoding error: {source}"),
        GetError::IrpcSend { source, .. } => eprintln!("error sending over irpc: {source}"),
        GetError::AtClosingNext { source, .. } => eprintln!("error at closing: {source}"),
        GetError::BadRequest { .. } => eprintln!("bad request"),
        GetError::LocalFailure { source, .. } => eprintln!("local failure {source:?}"),
    }
    e
}

/// Execute a receive operation, reporting progress through the channel.
pub async fn receive_files(
    config: ReceiveConfig,
    progress: mpsc::Sender<ReceiveProgress>,
) -> anyhow::Result<()> {
    let ticket = config.ticket;
    let addr = ticket.addr().clone();
    let secret_key = get_or_create_secret(config.verbose > 0)?;
    let mut builder = Endpoint::builder(presets::N0)
        .alpns(vec![])
        .secret_key(secret_key)
        .relay_mode(config.relay_mode.into());

    if ticket.addr().relay_urls().next().is_none() && ticket.addr().ip_addrs().next().is_none() {
        builder = builder.address_lookup(DnsAddressLookup::n0_dns());
    }
    if let Some(addr) = config.magic_ipv4_addr {
        builder = builder.bind_addr(addr)?;
    }
    if let Some(addr) = config.magic_ipv6_addr {
        builder = builder.bind_addr(addr)?;
    }
    let endpoint = builder.bind().await?;
    let dir_name = format!(".sendme-recv-{}", ticket.hash().to_hex());
    let iroh_data_dir = std::env::current_dir()?.join(dir_name);
    let db = iroh_blobs::store::fs::FsStore::load(&iroh_data_dir).await?;
    let db2 = db.clone();
    trace!("load done!");

    let fut = async {
        trace!("running");
        let hash_and_format = ticket.hash_and_format();
        trace!("computing local");
        let local = db.remote().local(hash_and_format).await?;
        trace!("local done");
        let (stats, total_files, payload_size) = if !local.is_complete() {
            trace!("{} not complete", hash_and_format.hash);
            progress
                .send(ReceiveProgress::Connecting)
                .await
                .ok();
            let connection = endpoint.connect(addr, iroh_blobs::protocol::ALPN).await?;
            progress
                .send(ReceiveProgress::GettingSizes)
                .await
                .ok();
            let (_hash_seq, sizes) =
                get_hash_seq_and_sizes(&connection, &hash_and_format.hash, 1024 * 1024 * 32, None)
                    .await
                    .map_err(show_get_error)?;
            let total_size = sizes.iter().copied().sum::<u64>();
            let payload_size = sizes.iter().skip(2).copied().sum::<u64>();
            let total_files = (sizes.len().saturating_sub(1)) as u64;
            progress
                .send(ReceiveProgress::CollectionInfo {
                    total_files,
                    total_size: payload_size,
                })
                .await
                .ok();

            let (tx, mut rx) = mpsc::channel(32);
            let local_size = local.local_bytes();
            let get = db.remote().execute_get(connection, local.missing());
            let progress_clone = progress.clone();
            let download_task = tokio::spawn(async move {
                while let Some(offset) = rx.recv().await {
                    progress_clone
                        .send(ReceiveProgress::Downloading {
                            progress: local_size + offset,
                            total: total_size,
                        })
                        .await
                        .ok();
                }
            });
            let mut stats = Stats::default();
            let mut stream = get.stream();
            while let Some(item) = stream.next().await {
                trace!("got item {item:?}");
                match item {
                    GetProgressItem::Progress(offset) => {
                        tx.send(offset).await.ok();
                    }
                    GetProgressItem::Done(value) => {
                        stats = value;
                        break;
                    }
                    GetProgressItem::Error(cause) => {
                        anyhow::bail!(show_get_error(cause));
                    }
                }
            }
            drop(tx);
            download_task.await.ok();
            (stats, total_files, payload_size)
        } else {
            let total_files = local.children().unwrap() - 1;
            (Stats::default(), total_files, 0u64)
        };
        let collection = Collection::load(hash_and_format.hash, db.as_ref()).await?;
        export(&db, collection, progress.clone()).await?;
        anyhow::Ok((total_files, payload_size, stats))
    };

    let (total_files, payload_size, stats) = match fut.await {
        Ok(x) => {
            endpoint.close().await;
            x
        }
        Err(e) => {
            endpoint.close().await;
            db2.shutdown().await?;
            let _ = progress
                .send(ReceiveProgress::Error(format!("{e}")))
                .await;
            return Err(e);
        }
    };

    tokio::fs::remove_dir_all(iroh_data_dir).await?;

    progress
        .send(ReceiveProgress::Done {
            total_files,
            payload_size,
            elapsed: stats.elapsed,
        })
        .await
        .ok();

    Ok(())
}
