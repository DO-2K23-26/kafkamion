use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{error, info};
use crate::CONFIG;
use crate::models::entity::{Driver, Truck};
use crate::models::position::Position;
use crate::models::time_registration::TimeRegistration;
use crate::models::report::Report;

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
            error!("An error occurred while connecting to Kafka: {:?}", err);
            false
        }
    }
}

/// Enum representing different types of events.
#[derive(Debug)]
enum Event {
    Driver(Driver),
    Truck(Truck),
    TimeRegistration(TimeRegistration),
    Position(Position),
    Unknown,
}

/// Determines the event type and returns the corresponding structure.
fn detect_event_type_topic(payload: &Value) -> Event {
    match payload.get("type_").and_then(Value::as_str) {
        Some("driver") => Event::Driver(Driver {
            type_: "driver".to_string(),
            driver_id: payload.get("driver_id").and_then(Value::as_str).map(String::from).expect("REASON"),
            first_name: payload.get("first_name").and_then(Value::as_str).map(String::from).expect("REASON"),
            last_name: payload.get("last_name").and_then(Value::as_str).map(String::from).expect("REASON"),
            email: payload.get("email").and_then(Value::as_str).map(String::from).expect("REASON"),
            phone: payload.get("phone").and_then(Value::as_str).map(String::from).expect("REASON"),
        }),
        Some("truck") => Event::Truck(Truck {
            type_: "truck".to_string(),
            truck_id: payload.get("truck_id").and_then(Value::as_str).map(String::from).expect("REASON"),
            immatriculation: payload.get("immatriculation").and_then(Value::as_str).map(String::from).expect("REASON"),
        }),
        Some("start") | Some("end") | Some("rest") => Event::TimeRegistration(TimeRegistration {
            type_: payload.get("type_").and_then(Value::as_str).unwrap_or_default().to_string(),
            timestamp: payload.get("timestamp").and_then(Value::as_str).map(String::from).expect("REASON"),
            driver_id: payload.get("driver_id").and_then(Value::as_str).map(String::from).expect("REASON"),
            truck_id: payload.get("truck_id").and_then(Value::as_str).map(String::from).expect("REASON"),
        }),
        Some("position") => Event::Position(Position {
            type_: "position".to_string(),
            truck_id: payload.get("truck_id").and_then(Value::as_str).map(String::from).expect("REASON"),
            latitude: payload.get("latitude").and_then(Value::as_f64).expect("REASON"),
            longitude: payload.get("longitude").and_then(Value::as_f64).expect("REASON"),
            timestamp: payload.get("timestamp").and_then(Value::as_str).map(String::from).expect("REASON"),
        }),
        _ => Event::Unknown,
    }
}

/// Kafka consumer for processing events.
#[tokio::main]
pub async fn consumer(client_config: ClientConfig) {
    tracing_subscriber::fmt::init();

    if !is_kafka_available(&client_config) {
        error!("Kafka is not available. Exiting...");
        return;
    }

    let shared_store: Arc<Mutex<HashMap<String, Report>>> = Arc::new(Mutex::new(HashMap::new()));

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
                if let Some(result) = consumer.poll(Duration::from_millis(1000)) {
                    match result {
                        Ok(message) => {
                            if let Some(payload) = message.payload() {
                                let payload_str = String::from_utf8_lossy(payload);

                                if let Ok(parsed_message) = serde_json::from_str::<Value>(&payload_str) {
                                    let event_type = detect_event_type_topic(&parsed_message);
                                    let key = get_key_from_message(&parsed_message);

                                    if key.is_empty() {
                                        error!("Message missing a key: {:?}", parsed_message);
                                        continue;
                                    }

                                    let mut store = shared_store.lock().await;
                                    let report = store.entry(key.clone()).or_default();

                                    match event_type {
                                        Event::Driver(ref driver) => {
                                            report.driver_id = driver.driver_id.clone();
                                            report.first_name = driver.first_name.clone();
                                            report.last_name = driver.last_name.clone();
                                            report.email = driver.email.clone();
                                            report.phone = driver.phone.clone();
                                        }
                                        Event::Truck(ref truck) => {
                                            report.truck_id = truck.truck_id.clone();
                                            report.immatriculation = truck.immatriculation.clone();
                                        }
                                        Event::TimeRegistration(ref time_reg) => match time_reg.type_.as_str() {
                                            "start" => {
                                                report.start_time = time_reg.timestamp.clone();
                                                report.driver_id = time_reg.driver_id.clone();
                                                report.truck_id = time_reg.truck_id.clone();
                                            }
                                            "end" => {
                                                report.end_time = time_reg.timestamp.clone();
                                                report.driver_id = time_reg.driver_id.clone();
                                                report.truck_id = time_reg.truck_id.clone();
                                            }
                                            "rest" => {
                                                report.rest_time = time_reg.timestamp.clone();
                                                report.driver_id = time_reg.driver_id.clone();
                                                report.truck_id = time_reg.truck_id.clone();
                                            }
                                            _ => {}
                                        },
                                        Event::Position(ref position) => match position.type_.as_str() {
                                            "start" => {
                                                report.latitude_start = position.latitude; 
                                                report.longitude_start = position.longitude; 
                                                report.timestamp_start = position.timestamp.clone();
                                            }
                                            "end" => {
                                                report.latitude_end = position.latitude; 
                                                report.longitude_end = position.longitude; 
                                                report.timestamp_end = position.timestamp.clone();
                                            }
                                            "rest" => {
                                                report.latitude_rest = position.latitude;
                                                report.longitude_rest = position.longitude; 
                                                report.timestamp_rest = position.timestamp.clone();
                                            }
                                            _ => {}
                                        },
                                        Event::Unknown => error!("Unknown event type: {:?}", parsed_message),
                                    }
                                    
                                    

                                    info!("Report for key {} and event type {:?} the full report is :  {:?}", key, event_type, report);

                                    if report.is_complete() {
                                        let completed_report = store.remove(&key); =

                                        if let Some(report) = completed_report {
                                            info!("Processing completed report: {:?}", report);
                                            process_report(report);
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => error!("Error reading message: {:?}", err),
                    }
                    
                } else {
                    sleep(Duration::from_millis(100)).await;
                }
            }
        });
    }

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl-c signal");
}

/// Extracts a key (e.g., truck_id or driver_id) from the payload.
fn get_key_from_message(payload: &Value) -> String {
    payload
        .get("truck_id")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| payload.get("driver_id").and_then(Value::as_str).unwrap_or_default().to_string())
}

/// Processes a completed report.
fn process_report(report: Report) {
    info!("Processing completed report: {:?}", report);
    // Add logic to save or forward the report.
}
