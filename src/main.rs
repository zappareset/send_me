//! sendme — GUI application for sending and receiving files over the network.
//!
//! Uses the iced GUI library with tokio for async network operations.

mod logic;
mod ui;

use ui::App;

fn main() -> iced::Result {
    tracing_subscriber::fmt::init();
    iced::application(App::title, App::update, App::view)
        .run_with(|| (App::new(), iced::Task::none()))
}
