use crate::config::CONFIG;
use log::{error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Checks if Kafka is available by attempting to connect to the server.
/// Returns `true` if the connection is successful, otherwise `false`.
pub fn is_kafka_available(client_config: &ClientConfig) -> bool {
    match client_config.create::<BaseConsumer>() {
        Ok(consumer) => {
            if consumer.fetch_metadata(None, Duration::from_secs(3)).is_ok() {
                info!("Kafka is reachable!");
                true
            } else {
                error!("Kafka is unreachable!");
                false
            }
        }
        Err(err) => {
            error!("An error has occurred while trying to connect to Kafka: {:?}", err);
            false
        }
    }
}

#[derive(Debug)]
enum EventType {
    Entity(EntityType),
    TimeRegistration,
    Position,
    Unknown,
}

#[derive(Debug)]
enum EntityType {
    Driver,
    Truck,
}

/// Detects the type of event based on the given payload.
fn detect_event_type_topic1(payload: &Value) -> EventType {
    if payload.get("driver_id").is_some() && payload.get("first_name").is_some() {
        EventType::Entity(EntityType::Driver)
    } else if payload.get("truck_id").is_some() && payload.get("immatriculation").is_some() {
        EventType::Entity(EntityType::Truck)
    } else {
        EventType::Unknown
    }
}

fn detect_event_type_topic2(payload: &Value) -> EventType {
   if payload.get("truck_id").is_some() && payload.get("latitude").is_some() {
        EventType::Position
    } else {
        EventType::Unknown
    }
}

fn detect_event_type_topic3(payload: &Value) -> EventType {
    if payload.get("timestamp").is_some() && payload.get("driver_id").is_some() {
        EventType::TimeRegistration
    } else {
        EventType::Unknown
    }
}

pub fn consumer(client_config: ClientConfig) {
    // Log the configuration
    info!("Configuration: {:#?}", client_config);

    if !is_kafka_available(&client_config) {
        error!("Kafka is not available. Exiting...");
        return;
    }

    // Shared HashMap for storing parsed messages
    let shared_store: Arc<Mutex<HashMap<String, Vec<Value>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Spawn a thread for each topic
    for topic in &CONFIG.topics {
        let client_config = client_config.clone();
        let shared_store = Arc::clone(&shared_store);
        let topic = topic.clone();

        thread::spawn(move || {
            let consumer: BaseConsumer = client_config.create().expect("Consumer creation failed");

            consumer
                .subscribe(&[&topic])
                .expect("Subscription to topic failed");

            loop {
                match consumer.poll(Duration::from_millis(1000)) {
                    Some(Ok(message)) => {
                        if let Some(payload) = message.payload() {
                            let payload_str = String::from_utf8_lossy(payload);

                            // Parse the message as JSON
                            match serde_json::from_str::<Value>(&payload_str) {
                                Ok(parsed_message) => {
                                    let event_type = if topic == "entity_topic" {
                                        detect_event_type_topic1(&parsed_message)
                                    } else if topic == "time_registration_topic" {
                                        detect_event_type_topic2(&parsed_message)
                                    } else if topic == "position_topic" {
                                        detect_event_type_topic3(&parsed_message)
                                    } else {
                                        EventType::Unknown
                                    };
                                    let event_key = match event_type {
                                        EventType::Entity(EntityType::Driver) => "entity_driver",
                                        EventType::Entity(EntityType::Truck) => "entity_truck",
                                        EventType::TimeRegistration => "time_registration",
                                        EventType::Position => "position",
                                        EventType::Unknown => "unknown",
                                    };

                                    // Store the parsed message in the shared store
                                    {
                                        let mut store = shared_store.lock().unwrap();
                                        let entry = store.entry(event_key.to_string()).or_insert_with(Vec::new);
                                        entry.push(parsed_message.clone());
                                    }

                                    println!("Parsed event ({}) from {}: {:?}", event_key, topic, parsed_message);
                                }
                                Err(err) => {
                                    eprintln!("Failed to parse message from {}: {}", topic, err);
                                }
                            }
                        }
                    }
                    Some(Err(err)) => {
                        eprintln!("Error in topic {}: {:?}", topic, err);
                    }
                    None => {
                        // println!("No message received from topic: {}", topic);
                    }
                }
            }
        });
    }

    // Prevent main thread from exiting
    loop {
        thread::park();
    }
}
