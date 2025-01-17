use std::{process::ExitCode, sync::Arc, time::Duration};

use clap::{Parser, Subcommand};
use tracing::info;

use crate::{
    events::{
        driver::DriverEvent, generate_and_publish, generate_and_publish_as_group,
        position::PositionEvent, time_registration::TimeRegistrationEvent, truck::TruckEvent,
    },
    kafka::KafkaClient,
};

pub mod scenarios;

#[derive(Debug, Parser)]
#[clap(name = "producer", version)]
pub struct App {
    #[clap(subcommand)]
    pub action: Command,

    #[clap(short, long, default_value_t = 100)]
    pub count: i32,

    #[clap(short, long, default_value_t = 30)]
    pub duration: u64,

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
    Run
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
                generate_and_publish(
                    event_generator,
                    client.clone(),
                    app.count,
                    "entity_topic",
                    "truck",
                )
                .await;
                Ok(ExitCode::SUCCESS)
            }
            Command::Driver => {
                let event_generator = Arc::new(DriverEvent::new());
                generate_and_publish(
                    event_generator,
                    client.clone(),
                    app.count,
                    "entity_topic",
                    "driver",
                )
                .await;
                Ok(ExitCode::SUCCESS)
            }
            Command::Position => {
                let event_generator = Arc::new(DriverEvent::new());
                let drivers = generate_and_publish(
                    event_generator,
                    client.clone(),
                    app.count,
                    "entity_topic",
                    "driver",
                )
                .await;
                // same with trucks
                let event_generator = Arc::new(TruckEvent::new());
                let trucks = generate_and_publish(
                    event_generator,
                    client.clone(),
                    app.count,
                    "entity_topic",
                    "truck",
                )
                .await;

                let event_generator = Arc::new(TimeRegistrationEvent::new(drivers, trucks));
                let time_registrations = generate_and_publish_as_group(
                    event_generator,
                    client.clone(),
                    app.count,
                    "entity_topic",
                    "time_registration",
                )
                .await;

                let event_generator = Arc::new(PositionEvent::new(time_registrations));
                generate_and_publish(
                    event_generator,
                    client,
                    app.count,
                    "position_topic",
                    "position",
                ).await;

                Ok(ExitCode::SUCCESS)
            }
            Command::TimeRegistration => {
                //generating drivers :
                let event_generator = Arc::new(DriverEvent::new());
                let drivers = generate_and_publish(
                    event_generator,
                    client.clone(),
                    app.count,
                    "entity_topic",
                    "driver",
                )
                .await;

                // same with trucks
                let event_generator = Arc::new(TruckEvent::new());
                let trucks = generate_and_publish(
                    event_generator,
                    client.clone(),
                    app.count,
                    "entity_topic",
                    "truck",
                )
                .await;

                let event_generator = Arc::new(TimeRegistrationEvent::new(drivers, trucks));
                generate_and_publish(
                    event_generator,
                    client,
                    app.count,
                    "time_registration_topic",
                    "time_registration",
                )
                .await;
                Ok(ExitCode::SUCCESS)
            }
            Command::Run => {
                let duration = Duration::from_secs(app.duration);
                //let task = task::spawn(async {
                //
                //})

                Ok(ExitCode::SUCCESS)
            }
        }
    }
}
