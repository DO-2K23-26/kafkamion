use std::{process::ExitCode, sync::Arc};

use clap::{Parser, Subcommand};
use tracing::info;

use crate::{
    events::{driver::DriverEvent, time_registration::TimeRegistrationEvent, truck::TruckEvent, EventSource},
    kafka::KafkaClient,
};

#[derive(Debug, Parser)]
#[clap(name = "producer", version)]
pub struct App {
    #[clap(subcommand)]
    pub action: Command,

    #[clap(short, long, default_value_t = 100)]
    pub count: i32,

    #[warn(unused_parens)]
    #[clap(short, long, default_value_t = ("localhost:9092".to_string()))]
    pub endpoint: String,
}

#[derive(Debug, Subcommand, PartialEq, Eq, Hash)]
pub enum Command {
    Driver,
    Truck,
    TimeRegistration,
    Position,
}

pub struct Cli {}

impl Cli {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn execute(&self, app: App) -> Result<ExitCode, ExitCode> {
        info!("Execution...");
        let client = Arc::new(KafkaClient::new(&app.endpoint));

        info!("Connected to consumer at {}", app.endpoint);
        match app.action {
            Command::Truck => {
                let event_generator = Arc::new(TruckEvent::new());
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let data = event_generator.generate();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("entity_topic", &message, "truck").await;
                    }
                }
                Ok(ExitCode::SUCCESS)
            }
            Command::Driver => {
                let event_generator = Arc::new(DriverEvent::new());
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let data = event_generator.generate();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("entity_topic", &message, "driver").await;
                    }
                }
                Ok(ExitCode::SUCCESS)
            }
            Command::Position => {
                todo!("to do");
            }
            Command::TimeRegistration => {
                let event_generator = Arc::new(TimeRegistrationEvent::new());
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let data = event_generator.generate();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("entity_topic", &message, "time_registration").await;
                    }
                }
                Ok(ExitCode::SUCCESS)
            }
        }
    }
}
