
//! GUI interface for sendme using the iced library.
//!
//! Provides two tabs: Send and Receive, with progress feedback and
//! async operation support via tokio channels bridged to iced Tasks.

use std::path::PathBuf;
use std::str::FromStr;

use iced::{
    widget::{button, column, container, horizontal_rule, pick_list, row, scrollable, text, text_input},
    Alignment, Element, Length, Task,
};

use crate::logic::{
    AddrInfoOptions, Format, ReceiveConfig, ReceiveProgress, RelayModeOption,
    SendConfig, SendProgress,
};

// ── Application state ────────────────────────────────────────────────────

pub struct App {
    /// Which tab is active.
    pub mode: Mode,
    /// State for the Send tab.
    pub send: SendTab,
    /// State for the Receive tab.
    pub receive: ReceiveTab,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Send,
    Receive,
}

impl Mode {
    fn all() -> [Mode; 2] {
        [Mode::Send, Mode::Receive]
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Send => write!(f, "Send"),
            Mode::Receive => write!(f, "Receive"),
        }
    }
}

// ── Send tab state ───────────────────────────────────────────────────────

pub struct SendTab {
    pub path: Option<PathBuf>,
    pub ticket_type: AddrInfoOptions,
    pub relay_mode: RelayModeStr,
    pub custom_relay_url: String,
    pub running: bool,
    pub log: Vec<String>,
    pub progress_pct: f32,
    pub status: String,
    pub ticket: Option<String>,
    pub hash_str: Option<String>,
    pub size_str: Option<String>,
    /// Channel to signal shutdown to the background send task.
    pub shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayModeStr {
    Default,
    Disabled,
    Custom,
}

impl RelayModeStr {
    fn all() -> &'static [RelayModeStr] {
        &[RelayModeStr::Default, RelayModeStr::Disabled, RelayModeStr::Custom]
    }
}

impl std::fmt::Display for RelayModeStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelayModeStr::Default => write!(f, "Default"),
            RelayModeStr::Disabled => write!(f, "Disabled"),
            RelayModeStr::Custom => write!(f, "Custom URL"),
        }
    }
}

impl Default for SendTab {
    fn default() -> Self {
        Self {
            path: None,
            ticket_type: AddrInfoOptions::RelayAndAddresses,
            relay_mode: RelayModeStr::Default,
            custom_relay_url: String::new(),
            running: false,
            log: Vec::new(),
            progress_pct: 0.0,
            status: String::from("Ready"),
            ticket: None,
            hash_str: None,
            size_str: None,
            shutdown_tx: None,
        }
    }
}

// ── Receive tab state ────────────────────────────────────────────────────

pub struct ReceiveTab {
    pub ticket_text: String,
    pub running: bool,
    pub log: Vec<String>,
    pub progress_pct: f32,
    pub status: String,
    pub done_message: Option<String>,
}

impl Default for ReceiveTab {
    fn default() -> Self {
        Self {
            ticket_text: String::new(),
            running: false,
            log: Vec::new(),
            progress_pct: 0.0,
            status: String::from("Ready"),
            done_message: None,
        }
    }
}

// ── Messages ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Message {
    // Tab selection
    SelectMode(Mode),

    // Send tab
    PickPath,
    PathSelected(Option<PathBuf>),
    TicketTypeChanged(AddrInfoOptions),
    RelayModeChanged(RelayModeStr),
    CustomRelayUrlChanged(String),
    StartSend,
    SendProgress(SendProgress),
    CopyTicket,
    StopSharing,

    // Receive tab
    TicketTextChanged(String),
    StartReceive,
    ReceiveProgress(ReceiveProgress),
}

// ── App implementation ───────────────────────────────────────────────────

impl App {
    pub fn new() -> Self {
        Self {
            mode: Mode::Send,
            send: SendTab::default(),
            receive: ReceiveTab::default(),
        }
    }

    pub fn title(&self) -> String {
        String::from("sendme")
    }

    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::SelectMode(mode) => {
                self.mode = mode;
                Task::none()
            }

            // ── Send tab ──────────────────────────────────────────────

            Message::PickPath => Task::perform(
                async {
                    // Try folder first, then file
                    if let Some(folder) = rfd::AsyncFileDialog::new()
                        .pick_folder()
                        .await
                    {
                        return Some(folder.path().to_path_buf());
                    }
                    if let Some(file) = rfd::AsyncFileDialog::new()
                        .pick_file()
                        .await
                    {
                        return Some(file.path().to_path_buf());
                    }
                    None
                },
                Message::PathSelected,
            ),

            Message::PathSelected(path) => {
                if let Some(ref p) = path {
                    self.send.status = format!("Selected: {}", p.display());
                }
                self.send.path = path;
                Task::none()
            }

            Message::TicketTypeChanged(tt) => {
                self.send.ticket_type = tt;
                Task::none()
            }

            Message::RelayModeChanged(rm) => {
                self.send.relay_mode = rm;
                Task::none()
            }

            Message::CustomRelayUrlChanged(url) => {
                self.send.custom_relay_url = url;
                Task::none()
            }

            Message::StartSend => {
                if self.send.running {
                    return Task::none();
                }
                let path = match &self.send.path {
                    Some(p) => p.clone(),
                    None => {
                        self.send.status =
                            String::from("Please select a file or folder first");
                        return Task::none();
                    }
                };

                let relay_mode = match self.send.relay_mode {
                    RelayModeStr::Default => RelayModeOption::Default,
                    RelayModeStr::Disabled => RelayModeOption::Disabled,
                    RelayModeStr::Custom => {
                        if self.send.custom_relay_url.is_empty() {
                            self.send.status =
                                String::from("Please enter a custom relay URL");
                            return Task::none();
                        }
                        match RelayModeOption::from_str(&self.send.custom_relay_url) {
                            Ok(rm) => rm,
                            Err(e) => {
                                self.send.status =
                                    format!("Invalid relay URL: {e}");
                                return Task::none();
                            }
                        }
                    }
                };

                let config = SendConfig {
                    path,
                    ticket_type: self.send.ticket_type,
                    relay_mode,
                    format: Format::Hex,
                    magic_ipv4_addr: None,
                    magic_ipv6_addr: None,
                    jobs: None,
                    verbose: 0,
                    show_secret: false,
                };

                self.send.running = true;
                self.send.log.clear();
                self.send.progress_pct = 0.0;
                self.send.status = String::from("Starting...");
                self.send.ticket = None;
                self.send.hash_str = None;
                self.send.size_str = None;

                // Create channels
                let (progress_tx, progress_rx) =
                    tokio::sync::mpsc::channel::<SendProgress>(32);
                let (shutdown_tx, shutdown_rx) =
                    tokio::sync::oneshot::channel::<()>();

                self.send.shutdown_tx = Some(shutdown_tx);

                // Spawn the background send task
                tokio::spawn(async move {
                    match crate::logic::send_files(config, progress_tx).await {
                        Ok(handle) => {
                            // Wait for shutdown signal
                            let _ = shutdown_rx.await;
                            let _ = handle.shutdown().await;
                        }
                        Err(_e) => {
                            // Error already sent via progress channel
                        }
                    }
                });

                // Convert receiver to stream and pipe to UI
                Task::run(
                    tokio_stream::wrappers::ReceiverStream::new(progress_rx),
                    Message::SendProgress,
                )
            }

            Message::SendProgress(progress) => {
                let done = matches!(&progress, SendProgress::Done);
                match progress {
                    SendProgress::ImportStarted { total_files } => {
                        self.send.log.push(format!(
                            "Importing {} file{}...",
                            total_files,
                            if total_files == 1 { "" } else { "s" }
                        ));
                        self.send.status =
                            format!("Importing {} files...", total_files);
                    }
                    SendProgress::ImportFile {
                        name,
                        progress,
                        size,
                    } => {
                        if size > 0 {
                            self.send.progress_pct =
                                (progress as f32 / size as f32) * 100.0;
                        }
                        self.send.status = format!(
                            "Importing: {} ({}/{})",
                            name, progress, size
                        );
                    }
                    SendProgress::ImportDone {
                        hash,
                        size,
                        elapsed: _,
                    } => {
                        let hash_str =
                            crate::logic::print_hash(&hash, Format::Hex);
                        self.send.hash_str = Some(hash_str.clone());
                        self.send.size_str = Some(human_bytes(size));
                        self.send.log.push(format!(
                            "Import complete: hash {}, size {}",
                            hash_str,
                            human_bytes(size)
                        ));
                        self.send.status =
                            String::from("Waiting for connections...");
                    }
                    SendProgress::WaitingForConnection => {
                        self.send.log.push(String::from(
                            "Endpoint online, waiting for connections",
                        ));
                        self.send.status =
                            String::from("Online — share the ticket below");
                    }
                    SendProgress::ClientConnected {
                        connection_id,
                        remote_id,
                    } => {
                        self.send.log.push(format!(
                            "Client connected: #{}, remote: {}",
                            connection_id, remote_id
                        ));
                        self.send.status =
                            format!("Transferring to {}...", remote_id);
                    }
                    SendProgress::TransferProgress {
                        request_id: _,
                        progress,
                        total,
                    } => {
                        if total > 0 {
                            self.send.progress_pct =
                                (progress as f32 / total as f32) * 100.0;
                        }
                        // Keep status from connection
                    }
                    SendProgress::TransferDone { .. } => {
                        self.send.progress_pct = 100.0;
                    }
                    SendProgress::ConnectionClosed { connection_id } => {
                        self.send.log.push(format!(
                            "Connection #{} closed",
                            connection_id
                        ));
                    }
                    SendProgress::TicketReady {
                        ticket,
                        hash,
                        size,
                    } => {
                        self.send.ticket = Some(ticket.clone());
                        self.send.hash_str =
                            Some(crate::logic::print_hash(&hash, Format::Hex));
                        self.send.size_str = Some(human_bytes(size));
                        self.send.log
                            .push(String::from("Ticket is ready!"));
                        self.send.log.push(format!("  {}", ticket));
                        self.send.status =
                            String::from("Ready — waiting for receiver");
                    }
                    SendProgress::ShuttingDown => {
                        self.send
                            .log
                            .push(String::from("Shutting down..."));
                        self.send.status = String::from("Shutting down...");
                    }
                    SendProgress::Done => {
                        // Handled below
                    }
                }
                if done {
                    self.send.running = false;
                    self.send.status = String::from("Done");
                }
                Task::none()
            }

            Message::CopyTicket => {
                if let Some(ref ticket) = self.send.ticket {
                    match arboard::Clipboard::new() {
                        Ok(mut c) => {
                            let _ = c.set_text(ticket);
                            self.send.status =
                                String::from("Ticket copied to clipboard!");
                        }
                        Err(e) => {
                            self.send.status =
                                format!("Failed to copy: {e}");
                        }
                    }
                }
                Task::none()
            }

            Message::StopSharing => {
                if let Some(tx) = self.send.shutdown_tx.take() {
                    self.send
                        .log
                        .push(String::from("Stopping sharing..."));
                    self.send.status = String::from("Shutting down...");
                    let _ = tx.send(());
                }
                Task::none()
            }

            // ── Receive tab ───────────────────────────────────────────

            Message::TicketTextChanged(text) => {
                self.receive.ticket_text = text;
                Task::none()
            }

            Message::StartReceive => {
                if self.receive.running {
                    return Task::none();
                }
                let ticket_str = self.receive.ticket_text.trim().to_string();
                if ticket_str.is_empty() {
                    self.receive.status =
                        String::from("Please enter a ticket");
                    return Task::none();
                }
                let ticket = match iroh_blobs::ticket::BlobTicket::from_str(
                    &ticket_str,
                ) {
                    Ok(t) => t,
                    Err(e) => {
                        self.receive.status =
                            format!("Invalid ticket: {e}");
                        return Task::none();
                    }
                };

                let config = ReceiveConfig {
                    ticket,
                    relay_mode: RelayModeOption::Default,
                    format: Format::Hex,
                    magic_ipv4_addr: None,
                    magic_ipv6_addr: None,
                    verbose: 0,
                };

                self.receive.running = true;
                self.receive.log.clear();
                self.receive.progress_pct = 0.0;
                self.receive.status = String::from("Starting...");
                self.receive.done_message = None;

                let (progress_tx, progress_rx) =
                    tokio::sync::mpsc::channel::<ReceiveProgress>(32);

                tokio::spawn(async move {
                    if let Err(e) =
                        crate::logic::receive_files(config, progress_tx).await
                    {
                        // Error should have been sent via progress channel
                        let _ = e;
                    }
                });

                Task::run(
                    tokio_stream::wrappers::ReceiverStream::new(progress_rx),
                    Message::ReceiveProgress,
                )
            }

            Message::ReceiveProgress(progress) => {
                match progress {
                    ReceiveProgress::Connecting => {
                        self.receive.log.push(String::from(
                            "Connecting to sender...",
                        ));
                        self.receive.status = String::from("Connecting...");
                    }
                    ReceiveProgress::GettingSizes => {
                        self.receive.log.push(String::from(
                            "Getting file sizes...",
                        ));
                        self.receive.status =
                            String::from("Getting file list...");
                    }
                    ReceiveProgress::CollectionInfo {
                        total_files,
                        total_size,
                    } => {
                        self.receive.log.push(format!(
                            "Receiving {} file{}, total size {}",
                            total_files,
                            if total_files == 1 { "" } else { "s" },
                            human_bytes(total_size)
                        ));
                        self.receive.status = format!(
                            "Downloading {} file{} ({} total)...",
                            total_files,
                            if total_files == 1 { "" } else { "s" },
                            human_bytes(total_size)
                        );
                    }
                    ReceiveProgress::Downloading { progress, total } => {
                        if total > 0 {
                            self.receive.progress_pct =
                                (progress as f32 / total as f32) * 100.0;
                        }
                        self.receive.status = format!(
                            "Downloading: {} / {}",
                            human_bytes(progress),
                            human_bytes(total)
                        );
                    }
                    ReceiveProgress::ExportStarted { total_files } => {
                        self.receive.log.push(format!(
                            "Exporting {} file{}...",
                            total_files,
                            if total_files == 1 { "" } else { "s" }
                        ));
                        self.receive.status =
                            format!("Exporting {} files...", total_files);
                    }
                    ReceiveProgress::ExportFile {
                        name,
                        progress,
                        size,
                    } => {
                        if size > 0 {
                            self.receive.progress_pct =
                                (progress as f32 / size as f32) * 100.0;
                        }
                        self.receive.status = format!(
                            "Exporting: {} ({}/{})",
                            name, progress, size
                        );
                    }
                    ReceiveProgress::Done {
                        total_files,
                        payload_size,
                        elapsed,
                    } => {
                        let msg = format!(
                            "Done! Received {} file{} ({}), took {:.1}s",
                            total_files,
                            if total_files == 1 { "" } else { "s" },
                            human_bytes(payload_size),
                            elapsed.as_secs_f64()
                        );
                        self.receive.done_message = Some(msg.clone());
                        self.receive.log.push(msg);
                        self.receive.status = String::from("Done");
                        self.receive.running = false;
                        self.receive.progress_pct = 100.0;
                    }
                    ReceiveProgress::Error(e) => {
                        self.receive.log.push(format!("ERROR: {e}"));
                        self.receive.status = format!("Error: {e}");
                        self.receive.running = false;
                    }
                }
                Task::none()
            }

        }
    }

    pub fn view(&self) -> Element<'_, Message> {
        let mode_selector = row(
            Mode::all().iter().map(|mode| {
                let btn = button(text(mode.to_string()))
                    .on_press(Message::SelectMode(*mode));
                if self.mode == *mode {
                    btn.style(button::primary)
                } else {
                    btn
                }
                .into()
            }),
        )
        .spacing(4);

        let content: Element<_> = match self.mode {
            Mode::Send => self.view_send(),
            Mode::Receive => self.view_receive(),
        };

        column![
            text("sendme").size(24),
            mode_selector,
            horizontal_rule(1),
            content,
        ]
        .spacing(12)
        .padding(16)
        .into()
    }

    fn view_send(&self) -> Element<'_, Message> {
        // Path picker
        let path_row = row![
            button("Choose File or Folder").on_press_maybe(
                if self.send.running { None } else { Some(Message::PickPath) }
            ),
            text(match &self.send.path {
                Some(p) => p.display().to_string(),
                None => String::from("No file selected"),
            })
            .width(Length::Fill),
        ]
        .spacing(8)
        .align_y(Alignment::Center);

        // Options
        let options = column![
            text("Options:").size(16),
            row![
                text("Ticket type:").width(100),
                pick_list(
                    AddrInfoOptions::all_options(),
                    Some(self.send.ticket_type),
                    Message::TicketTypeChanged
                ),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
            row![
                text("Relay mode:").width(100),
                pick_list(
                    RelayModeStr::all(),
                    Some(self.send.relay_mode),
                    Message::RelayModeChanged
                ),
            ]
            .spacing(8)
            .align_y(Alignment::Center),
        ]
        .spacing(8);

        // Custom relay URL
        let custom_url_row = if self.send.relay_mode == RelayModeStr::Custom {
            Some(
                row![
                    text("Relay URL:").width(100),
                    text_input(
                        "https://relay.example.com",
                        &self.send.custom_relay_url
                    )
                    .on_input(Message::CustomRelayUrlChanged)
                    .width(Length::Fill),
                ]
                .spacing(8)
                .align_y(Alignment::Center),
            )
        } else {
            None
        };

        // Action button
        let action_button = if self.send.running
            && self.send.shutdown_tx.is_some()
        {
            button("Stop Sharing").on_press(Message::StopSharing)
        } else {
            button("Send").on_press_maybe(
                if self.send.running {
                    None
                } else {
                    Some(Message::StartSend)
                },
            )
        };

        // Info
        let info = if let Some(ref hash) = self.send.hash_str {
            let mut lines = vec![format!("Hash: {}", hash)];
            if let Some(ref size) = self.send.size_str {
                lines.push(format!("Size: {}", size));
            }
            Some(text(lines.join("  |  ")).size(12))
        } else {
            None
        };

        // Ticket
        let ticket_display = self.send.ticket.as_ref().map(|ticket| {
            column![
                text("Ticket (share this with the receiver):").size(14),
                container(
                    scrollable(text(ticket).size(12))
                        .height(Length::Fixed(40.0))
                )
                .style(container::rounded_box),
                button("Copy Ticket").on_press(Message::CopyTicket),
            ]
            .spacing(4)
        });

        // Progress bar
        let progress_bar = (self.send.running || self.send.progress_pct > 0.0)
            .then(|| {
                column![
                    text(format!("{:.0}%", self.send.progress_pct)).size(12),
                    iced::widget::progress_bar(
                        0.0..=100.0,
                        self.send.progress_pct,
                    ),
                ]
                .spacing(4)
            });

        // Log
        let log_view = (!self.send.log.is_empty()).then(|| {
            column![
                text("Log:").size(14),
                container(
                    scrollable(
                        column(
                            self.send
                                .log
                                .iter()
                                .map(|l| text(l).size(12).into())
                                .collect::<Vec<Element<_>>>(),
                        )
                        .spacing(2),
                    )
                    .height(Length::Fixed(120.0)),
                )
                .style(container::bordered_box),
            ]
            .spacing(4)
        });

        let mut col = column![
            text("Send Files").size(20),
            path_row,
            options,
        ]
        .spacing(12);

        if let Some(row) = custom_url_row {
            col = col.push(row);
        }
        col = col.push(action_button);
        if let Some(info) = info {
            col = col.push(info);
        }
        if let Some(ticket) = ticket_display {
            col = col.push(ticket);
        }
        if let Some(pb) = progress_bar {
            col = col.push(pb);
        }
        col = col.push(text(&self.send.status).size(14));
        if let Some(log) = log_view {
            col = col.push(log);
        }

        scrollable(col).into()
    }

    fn view_receive(&self) -> Element<'_, Message> {
        let ticket_row = row![
            text("Ticket:").width(80),
            text_input("paste ticket here...", &self.receive.ticket_text)
                .on_input(Message::TicketTextChanged)
                .width(Length::Fill),
        ]
        .spacing(8)
        .align_y(Alignment::Center);

        let action_button = button("Receive").on_press_maybe(
            if self.receive.running {
                None
            } else {
                Some(Message::StartReceive)
            },
        );

        let progress_bar =
            (self.receive.running || self.receive.progress_pct > 0.0).then(
                || {
                    column![
                        text(format!("{:.0}%", self.receive.progress_pct))
                            .size(12),
                        iced::widget::progress_bar(
                            0.0..=100.0,
                            self.receive.progress_pct,
                        ),
                    ]
                    .spacing(4)
                },
            );

        let done_msg = self
            .receive
            .done_message
            .as_ref()
            .map(|msg| text(msg).size(14));

        let log_view = (!self.receive.log.is_empty()).then(|| {
            column![
                text("Log:").size(14),
                container(
                    scrollable(
                        column(
                            self.receive
                                .log
                                .iter()
                                .map(|l| text(l).size(12).into())
                                .collect::<Vec<Element<_>>>(),
                        )
                        .spacing(2),
                    )
                    .height(Length::Fixed(120.0)),
                )
                .style(container::bordered_box),
            ]
            .spacing(4)
        });

        let mut col =
            column![text("Receive Files").size(20), ticket_row, action_button,]
                .spacing(12);

        if let Some(pb) = progress_bar {
            col = col.push(pb);
        }
        if let Some(msg) = done_msg {
            col = col.push(msg);
        }
        col = col.push(text(&self.receive.status).size(14));
        if let Some(log) = log_view {
            col = col.push(log);
        }

        scrollable(col).into()
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn human_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    format!("{:.1} {}", size, UNITS[unit_idx])
}

impl AddrInfoOptions {
    fn all_options() -> &'static [AddrInfoOptions] {
        &[
            AddrInfoOptions::RelayAndAddresses,
            AddrInfoOptions::Id,
            AddrInfoOptions::Relay,
            AddrInfoOptions::Addresses,
        ]
    }
}
