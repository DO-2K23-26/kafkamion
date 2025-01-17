use clap::Parser;
use cli::{App, Cli};
use tracing::info;

mod events;
mod cli;
mod kafka;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    info!("tracer init");
    let args = App::parse();

    let result = &Cli::new().execute(args).await;

}
