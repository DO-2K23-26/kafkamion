use std::{process::ExitCode, sync::Arc};

use clap::{Parser, Subcommand};
use tracing::info;

use crate::{
    events::{driver::DriverEvent, position::{self, PositionEvent}, time_registration::{self, TimeRegistrationEvent}, truck::TruckEvent, EventSource, ReusableEventSource},
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
                let event_generator = Arc::new(DriverEvent::new());
                let mut drivers = Vec::new();
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let (data, ids) = event_generator.generate_with_id();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("entity_topic", &message, "driver").await;
                    }
                    for driver in ids {
                        drivers.push(driver);
                    }
                }
                // same with trucks
                let event_generator = Arc::new(TruckEvent::new());
                let mut trucks = Vec::new();
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let (data, ids) = event_generator.generate_with_id();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("entity_topic", &message, "driver").await;
                    }
                    for truck in ids {
                        trucks.push(truck);
                    }
                }

                let event_generator = Arc::new(TimeRegistrationEvent::new(drivers, trucks));
                let mut time_registrations = Vec::new();
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let (data, time_registration) = event_generator.generate_with_id();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("time_registration_topic", &message, "time_registration").await;
                    }
                    time_registrations.push(time_registration);
                }

                let event_generator = Arc::new(PositionEvent::new(time_registrations));
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let data = event_generator.generate();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("position_topic", &message, "position").await;
                    }
                }


                Ok(ExitCode::SUCCESS)
            }
            Command::TimeRegistration => {
                //generating drivers : 
                let event_generator = Arc::new(DriverEvent::new());
                let mut drivers = Vec::new();
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let (data, ids) = event_generator.generate_with_id();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("entity_topic", &message, "driver").await;
                    }
                    for driver in ids {
                        drivers.push(driver);
                    }
                }
                // same with trucks
                let event_generator = Arc::new(TruckEvent::new());
                let mut trucks = Vec::new();
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let (data, ids) = event_generator.generate_with_id();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("entity_topic", &message, "driver").await;
                    }
                    for truck in ids {
                        trucks.push(truck);
                    }
                }

                let event_generator = Arc::new(TimeRegistrationEvent::new(drivers, trucks));
                for _ in 0..app.count {
                    let client = client.clone();
                    let event_generator = event_generator.clone();
                    let data = event_generator.generate();
                    info!("generated {:?}", data);
                    for message in data {
                        client.publish("time_registration_topic", &message, "time_registration").await;
                    }
                }
                Ok(ExitCode::SUCCESS)
            }
        }
    }
}
