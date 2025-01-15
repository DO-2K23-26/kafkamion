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

/// Extracts a key (e.g., truck_id or driver_id) from the payload.
fn get_key_from_message(payload: &Value) -> String {
    payload
        .get("truck_id")
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| payload.get("driver_id").and_then(Value::as_str).unwrap_or_default().to_string())
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

    let driver_store: Arc<Mutex<HashMap<String, Driver>>> = Arc::new(Mutex::new(HashMap::new()));
    let truck_store: Arc<Mutex<HashMap<String, Truck>>> = Arc::new(Mutex::new(HashMap::new()));

    let start_time_store: Arc<Mutex<HashMap<String, TimeRegistration>>> = Arc::new(Mutex::new(HashMap::new()));
    let end_time_store: Arc<Mutex<HashMap<String, TimeRegistration>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_time_store: Arc<Mutex<HashMap<String, TimeRegistration>>> = Arc::new(Mutex::new(HashMap::new()));

    let start_position_store: Arc<Mutex<HashMap<String, Position>>> = Arc::new(Mutex::new(HashMap::new()));
    let end_position_store: Arc<Mutex<HashMap<String, Position>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_position_store: Arc<Mutex<HashMap<String, Position>>> = Arc::new(Mutex::new(HashMap::new()));

    for topic in &CONFIG.topics {
        let client_config = client_config.clone();
        let shared_store = Arc::clone(&shared_store);
        let topic = topic.clone();

        let driver_store = Arc::clone(&driver_store);
        let truck_store = Arc::clone(&truck_store);

        let start_time_store = Arc::clone(&start_time_store);
        let end_time_store = Arc::clone(&end_time_store);
        let rest_time_store = Arc::clone(&rest_time_store);

        let start_position_store = Arc::clone(&start_position_store);
        let end_position_store = Arc::clone(&end_position_store);
        let rest_position_store = Arc::clone(&rest_position_store);

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
                                            driver_store.lock().await.insert(driver.driver_id.clone(), driver.clone());
                                            let driver_store_snapshot = driver_store.lock().await;
                                            info!("Updated Driver store: {:?}", *driver_store_snapshot);
                                        }
                                        Event::Truck(ref truck) => {
                                            truck_store.lock().await.insert(truck.truck_id.clone(), truck.clone());
                                            let truck_store_snapshot = truck_store.lock().await;
                                            info!("Updated Truck store: {:?}", *truck_store_snapshot);
                                        }
                                        Event::TimeRegistration(ref time_reg) => match time_reg.type_.as_str() {
                                            "start" => {
                                                start_time_store.lock().await.insert(time_reg.driver_id.clone(), time_reg.clone());
                                                let start_time_snapshot = start_time_store.lock().await;
                                                info!("Updated Start time store: {:?}", *start_time_snapshot);
                                            }
                                            "end" => {
                                                end_time_store.lock().await.insert(time_reg.driver_id.clone(), time_reg.clone());
                                                let end_time_snapshot = end_time_store.lock().await;
                                                info!("Updated End time store: {:?}", *end_time_snapshot);
                                            }
                                            "rest" => {
                                                rest_time_store.lock().await.insert(time_reg.driver_id.clone(), time_reg.clone());
                                                let rest_time_snapshot = rest_time_store.lock().await;
                                                info!("Updated Rest time store: {:?}", *rest_time_snapshot);
                                            }
                                            _ => {}
                                        },
                                        Event::Position(ref position) => {
                                            // Insert the position into the start, end, and rest position stores
                                            let mut start_position_store_lock = start_position_store.lock().await;
                                            start_position_store_lock.insert(position.truck_id.clone(), position.clone());
                                            info!("Updated Start position store: {:?}", start_position_store_lock);
                                        
                                            let mut end_position_store_lock = end_position_store.lock().await;
                                            end_position_store_lock.insert(position.truck_id.clone(), position.clone());
                                            info!("Updated End position store: {:?}", end_position_store_lock);
                                        
                                            let mut rest_position_store_lock = rest_position_store.lock().await;
                                            rest_position_store_lock.insert(position.truck_id.clone(), position.clone());
                                            info!("Updated Rest position store: {:?}", rest_position_store_lock);
                                        },
                                        Event::Unknown => error!("Unknown event type: {:?}", parsed_message),
                                    }

                                    if let Some(report) = get_matching_entry(
                                        &start_time_store,
                                        &end_time_store,
                                        &rest_time_store,
                                        &start_position_store,
                                        &end_position_store,
                                        &rest_position_store,
                                        &driver_store,
                                        &truck_store,
                                        &key,
                                    ).await {
                                        process_report(report);
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

pub async fn get_matching_entry(
    start_time_store: &Arc<Mutex<HashMap<String, TimeRegistration>>>,
    end_time_store: &Arc<Mutex<HashMap<String, TimeRegistration>>>,
    rest_time_store: &Arc<Mutex<HashMap<String, TimeRegistration>>>,
    start_position_store: &Arc<Mutex<HashMap<String, Position>>>,
    end_position_store: &Arc<Mutex<HashMap<String, Position>>>,
    rest_position_store: &Arc<Mutex<HashMap<String, Position>>>,
    driver_store: &Arc<Mutex<HashMap<String, Driver>>>,
    truck_store: &Arc<Mutex<HashMap<String, Truck>>>,
    key: &str,
) -> Option<Report> {
    let start_time = start_time_store.lock().await.get(key).cloned();
    let end_time = end_time_store.lock().await.get(key).cloned();
    let rest_time = rest_time_store.lock().await.get(key).cloned();

    let start_position = start_position_store.lock().await.get(key).cloned();
    let end_position = end_position_store.lock().await.get(key).cloned();
    let rest_position = rest_position_store.lock().await.get(key).cloned();

    let driver = driver_store.lock().await.get(key).cloned();
    let truck = truck_store.lock().await.get(key).cloned();

    if let (Some(driver), Some(truck), Some(start_time), Some(end_time), Some(rest_time), Some(start_position), Some(end_position), Some(rest_position)) = (
        driver, truck, start_time, end_time, rest_time, start_position, end_position, rest_position) {
        Some(Report {
            driver_id: driver.driver_id,
            first_name: driver.first_name,
            last_name: driver.last_name,
            email: driver.email,
            phone: driver.phone,
            truck_id: truck.truck_id,
            immatriculation: truck.immatriculation,
            start_time: start_time.timestamp,
            end_time: end_time.timestamp,
            rest_time: rest_time.timestamp,
            latitude_start: start_position.latitude,
            longitude_start: start_position.longitude,
            timestamp_start: start_position.timestamp,
            latitude_end: end_position.latitude,
            longitude_end: end_position.longitude,
            timestamp_end: end_position.timestamp,
            latitude_rest: rest_position.latitude,
            longitude_rest: rest_position.longitude,
            timestamp_rest: rest_position.timestamp,
        })
    } else {
        info!("No matching entry found for key: {}", key);
        None
    }
}


/// Processes a completed report.
fn process_report(report: Report) {
    info!("Processing completed report: {:?}", report);
    // Create the flat JSON representation of the report
    let report_json = serde_json::to_value(report).expect("Failed to serialize report");
    // You can now process the report_json further, like saving or forwarding it.
    info!("Flat report JSON: {:?}", report_json);
}