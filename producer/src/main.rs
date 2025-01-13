use clap::Parser;
use cli::{App, Cli, Command};
use events::driver::DriverEvent;

mod events;
mod cli;
mod kafka;

#[tokio::main]
async fn main() {
    let args = App::parse();

    let result = &Cli::new().execute(args).await;

    println!("Hello, world!");
}
