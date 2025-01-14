mod config;
mod consumer;
mod models;

use crate::config::CONFIG;
use crate::consumer::consumer;
use rdkafka::config::ClientConfig;
use models::entity::Entity;
use models::position::Position;
use models::time_registration::TimeRegistration;
use chrono::Utc;

fn main() {
    let entity = Entity::newDriver("driver".to_string(), "D-123".to_string(), "baptiste".to_string(), "bronsin".to_string(), "email".to_string(), "+330650353421".to_string());
    println!("{:?}", entity);
    let entity = Entity::newTruck("truck".to_string(), "T-123".to_string(), "123AB456".to_string());
    println!("{:?}", entity);
    let time_registration = TimeRegistration::new("start".to_string(), "2025-01-09T08:41:00Z".to_string(), "D-123".to_string(), "T-123".to_string());
    println!("{:?}", time_registration);
    let pos = Position::new("T-123".to_string(), 123.0, 456.0, Utc::now().to_string());
    println!("{:?}", pos);

    env_logger::init(); // Initialize the logger

    // Create a new client configuration
    let mut client_config = ClientConfig::new();

    // Set the group ID and bootstrap servers from the CONFIG struct
    client_config
        .set("group.id", &CONFIG.group_id)
        .set("bootstrap.servers", &CONFIG.kafka_broker);

    // Call the consumer function with the configured client configuration
    println!("Starting consumer...");
    consumer(client_config);
}
