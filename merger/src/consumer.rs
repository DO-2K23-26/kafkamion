use crate::config::CONFIG;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

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

fn detect_event_type_topic(payload: &Value) -> EventType {
    if let Some(type_value) = payload.get("type") {
        tracing::debug!("Type detected: {:?}", type_value);
    }

    match payload.get("type_") {
        Some(r#type) => match r#type.as_str() {
            Some("driver") => EventType::Entity(EntityType::Driver),
            Some("truck") => EventType::Entity(EntityType::Truck),
            Some("start") => EventType::TimeRegistration,
            Some("end") => EventType::TimeRegistration,
            Some("rest") => EventType::TimeRegistration,
            Some("position") => EventType::Position,
            _ => EventType::Unknown,
        },
        None => EventType::Unknown,
    }
}

#[tokio::main]
pub async fn consumer(client_config: ClientConfig) {
    tracing_subscriber::fmt::init();

    info!("Configuration: {:#?}", client_config);

    if !is_kafka_available(&client_config) {
        error!("Kafka is not available. Exiting...");
        return;
    }

    // Shared HashMap for storing parsed messages
    let shared_store: Arc<Mutex<HashMap<String, Vec<Value>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Spawn a task for each topic
    for topic in &CONFIG.topics {
        let client_config = client_config.clone();
        let shared_store = Arc::clone(&shared_store);
        let topic = topic.clone();

        tokio::spawn(async move {
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
                                    // Detect the event type
                                    let event_type = detect_event_type_topic(&parsed_message);

                                    let event_key = match event_type {
                                        EventType::Entity(EntityType::Driver) => "entity_driver",
                                        EventType::Entity(EntityType::Truck) => "entity_truck",
                                        EventType::TimeRegistration => "time_registration",
                                        EventType::Position => "position",
                                        EventType::Unknown => "unknown",
                                    };

                                    // Store the parsed message in the shared store
                                    {
                                        let mut store = shared_store.lock().await;
                                        let entry = store.entry(event_key.to_string()).or_insert_with(Vec::new);
                                        entry.push(parsed_message.clone());
                                    }

                                    info!("Parsed event ({}) from {}: {:?}", event_key, topic, parsed_message);
                                }
                                Err(err) => {
                                    error!("Failed to parse message from {}: {}", topic, err);
                                }
                            }
                        }
                    }
                    Some(Err(err)) => {
                        error!("Error in topic {}: {:?}", topic, err);
                    }
                    None => {
                        // Uncomment for heartbeat messages if needed
                        // debug!("No message received from topic: {}", topic);
                    }
                }

                // Sleep to prevent tight looping in case of no messages
                sleep(Duration::from_millis(100)).await;
            }
        });
    }

    // Wait for termination signal
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c signal");
}
