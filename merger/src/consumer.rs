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
use crate::models::position::{self, FullPosition, Position};
use crate::models::time_registration::{self, FullTimeRegistration, TimeRegistration};
use crate::models::report::Report;

use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;


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

// Check if the required data is available in each stores
async fn check_required_data(
    driver_id: &str,
    truck_id: &str,
    driver_store: &Arc<Mutex<HashMap<String, Driver>>>,
    truck_store: &Arc<Mutex<HashMap<String, Truck>>>,
    position_store: &Arc<Mutex<HashMap<String, FullPosition>>>,
    time_store: &Arc<Mutex<HashMap<String, FullTimeRegistration>>>,
) -> bool {
    let driver_store_lock = driver_store.lock().await;
    let truck_store_lock = truck_store.lock().await;
    let position_store_lock = position_store.lock().await;
    let time_store_lock = time_store.lock().await;

    // Check if the driver, truck, time and position exist in the stores
    driver_store_lock.contains_key(driver_id)
        && truck_store_lock.contains_key(truck_id)
        && position_store_lock.contains_key(truck_id)
        && time_store_lock.contains_key(driver_id)
}

async fn check_create_and_send_report(
    client_config: ClientConfig,
    driver_id: Option<&str>,
    truck_id: Option<&str>,
    driver_store: &Arc<Mutex<HashMap<String, Driver>>>,
    truck_store: &Arc<Mutex<HashMap<String, Truck>>>,
    position_store: &Arc<Mutex<HashMap<String, FullPosition>>>,
    time_store: &Arc<Mutex<HashMap<String, FullTimeRegistration>>>,
) -> bool {
    // Error if both driver_id and truck_id are None
    if driver_id.is_none() && truck_id.is_none() {
        return false;
    }

    let mut driver_id_set = driver_id.unwrap_or("").to_string();
    let mut truck_id_set = truck_id.unwrap_or("").to_string();

    if driver_id.is_some() && truck_id.is_some() {
        
    } else if driver_id.is_some() {
        let time_registration_lock = time_store.lock().await;
        let time_registration = time_registration_lock.get(driver_id.unwrap()).cloned().unwrap();
        
        if time_registration.start.is_some() {
            truck_id_set = time_registration.start.clone().unwrap().truck_id;
        } else if time_registration.end.is_some() {
            truck_id_set = time_registration.end.clone().unwrap().truck_id;
        } else if time_registration.rest.is_some() {
            truck_id_set = time_registration.rest.clone().unwrap().truck_id;
        } else {
            return false;
        }
    } else {
        let time_registration_lock = time_store.lock().await;
        for (driver_id, time_registration) in time_registration_lock.iter() {
            if time_registration.start.is_some() && time_registration.start.clone().unwrap().truck_id == truck_id.unwrap() {
                driver_id_set = driver_id.to_string();
            } else if time_registration.end.is_some() && time_registration.end.clone().unwrap().truck_id == truck_id.unwrap() {
                driver_id_set = driver_id.to_string();
            } else if time_registration.rest.is_some() && time_registration.rest.clone().unwrap().truck_id == truck_id.unwrap() {
                driver_id_set = driver_id.to_string();
            } else {
                return false;
            }
        } 
    }

    if !check_required_data(
        &driver_id_set,
        &truck_id_set,
        driver_store,
        truck_store,
        position_store,
        time_store,
    ).await {
        info!(
            "Waiting for required data for driver {} and truck {}",
            driver_id_set, truck_id_set
        );
        return false;
    }

    info!(
        "Required data for driver {} and truck {} is available",
        driver_id_set, truck_id_set
    );

    let driver_store_lock = driver_store.lock().await;
    let truck_store_lock = truck_store.lock().await;
    let time_store_lock = time_store.lock().await;
    let position_store_lock = position_store.lock().await;
    
    let driver = driver_store_lock.get(&driver_id_set).cloned().unwrap();
    let truck = truck_store_lock.get(&truck_id_set).cloned().unwrap();
    let time_registration = time_store_lock.get(&driver_id_set).cloned().unwrap();
    let position = position_store_lock.get(&truck_id_set).cloned().unwrap();

    if time_registration.start.is_none() ||  time_registration.rest.is_none() || time_registration.end.is_none() {
        info!("Waiting for all time registrations for driver {}", driver_id_set);
        return false;
    }

    if position.start.is_none() || position.rest.is_none() || position.end.is_none() {
        info!("Waiting for all positions for truck {}", truck_id_set);
        return false;
    }

    let report = Report::new(
        driver.driver_id.to_string(),
        driver.first_name.clone(),
        driver.last_name.clone(),
        driver.email.clone(),
        driver.phone.clone(),
        truck.truck_id.to_string(),
        truck.immatriculation.clone(),
        time_registration.start.unwrap().timestamp.clone(),
        time_registration.end.unwrap().timestamp.clone(),
        time_registration.rest.unwrap().timestamp.clone(),
        position.start.clone().unwrap().latitude,
        position.start.clone().unwrap().longitude,
        position.start.clone().unwrap().timestamp.clone(),
        position.end.clone().unwrap().latitude,
        position.end.clone().unwrap().longitude,
        position.end.clone().unwrap().timestamp.clone(),
        position.rest.clone().unwrap().latitude,
        position.rest.clone().unwrap().longitude,
        position.rest.clone().unwrap().timestamp.clone(),
    );

    info!("Created new Report: {:?}", report);

    push_to_kafka_topic(
        &client_config.create().expect("Producer creation failed"),
        "report_topic",
        &report.driver_id,
        &report,
    ).await.unwrap();

    true
}

/// Kafka consumer for processing events.
#[tokio::main]
pub async fn consumer(client_config: ClientConfig) {
    tracing_subscriber::fmt::init();

    if !is_kafka_available(&client_config) {
        error!("Kafka is not available. Exiting...");
        return;
    }

    // let shared_store: Arc<Mutex<HashMap<String, Report>>> = Arc::new(Mutex::new(HashMap::new()));

    let driver_store: Arc<Mutex<HashMap<String, Driver>>> = Arc::new(Mutex::new(HashMap::new()));
    let truck_store: Arc<Mutex<HashMap<String, Truck>>> = Arc::new(Mutex::new(HashMap::new()));

    let time_store: Arc<Mutex<HashMap<String, FullTimeRegistration>>> = Arc::new(Mutex::new(HashMap::new()));

    // let start_time_store: Arc<Mutex<HashMap<String, TimeRegistration>>> = Arc::new(Mutex::new(HashMap::new()));
    // let end_time_store: Arc<Mutex<HashMap<String, TimeRegistration>>> = Arc::new(Mutex::new(HashMap::new()));
    // let rest_time_store: Arc<Mutex<HashMap<String, TimeRegistration>>> = Arc::new(Mutex::new(HashMap::new()));

    let position_store: Arc<Mutex<HashMap<String, FullPosition>>> = Arc::new(Mutex::new(HashMap::new()));

    // let start_position_store: Arc<Mutex<HashMap<String, Position>>> = Arc::new(Mutex::new(HashMap::new()));
    // let end_position_store: Arc<Mutex<HashMap<String, Position>>> = Arc::new(Mutex::new(HashMap::new()));
    // let rest_position_store: Arc<Mutex<HashMap<String, Position>>> = Arc::new(Mutex::new(HashMap::new()));

    for topic in &CONFIG.topics {
        println!("Subscribing to topic: {}", topic);
        let client_config = client_config.clone();
        // let shared_store = Arc::clone(&shared_store);
        let topic = topic.clone();

        let driver_store = Arc::clone(&driver_store);
        let truck_store = Arc::clone(&truck_store);

        let time_store = Arc::clone(&time_store);
        // let start_time_store = Arc::clone(&start_time_store);
        // let end_time_store = Arc::clone(&end_time_store);
        // let rest_time_store = Arc::clone(&rest_time_store);

        let position_store = Arc::clone(&position_store);
        // let start_position_store = Arc::clone(&start_position_store);
        // let end_position_store = Arc::clone(&end_position_store);
        // let rest_position_store = Arc::clone(&rest_position_store);

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

                                    // let mut store = shared_store.lock().await;
                                    
                                    match event_type {
                                        Event::Driver(ref driver) => {
                                            driver_store.lock().await.insert(driver.driver_id.clone(), driver.clone());
                                            let driver_store_snapshot = driver_store.lock().await;
                                            info!("Updated Driver store: {:?}", *driver_store_snapshot);

                                            let time_reg = {
                                                let time_store_lock = time_store.lock().await;
                                                time_store_lock.get(&driver.driver_id).cloned()
                                            };

                                            if time_reg.is_none() {
                                                continue;
                                            }

                                            if check_create_and_send_report(
                                                client_config.clone(),
                                                Some(&driver.driver_id),
                                                None,
                                                &driver_store,
                                                &truck_store,
                                                &position_store,
                                                &time_store,
                                            ).await {
                                                info!("Report created and sent successfully");
                                            }
                                        }
                                        Event::Truck(ref truck) => {
                                            truck_store.lock().await.insert(truck.truck_id.clone(), truck.clone());
                                            let truck_store_snapshot = truck_store.lock().await;
                                            info!("Updated Truck store: {:?}", *truck_store_snapshot);

                                            if check_create_and_send_report(
                                                client_config.clone(),
                                                None,
                                                Some(&truck.truck_id),
                                                &driver_store,
                                                &truck_store,
                                                &position_store,
                                                &time_store,
                                            ).await {
                                                info!("Report created and sent successfully");
                                            }
                                        }
                                        Event::TimeRegistration(ref time_reg) => match time_reg.type_.as_str() {
                                            "start" => {
                                                {
                                                    let mut time_store_lock = time_store.lock().await;
                                                    let full_time_registration = time_store_lock.entry(time_reg.driver_id.clone()).or_insert(FullTimeRegistration::new());
                                                    full_time_registration.start = Some(time_reg.clone());
                                                    info!("Updated Time store with START time");

                                                    if check_create_and_send_report(
                                                        client_config.clone(),
                                                        Some(&time_reg.driver_id),
                                                        None,
                                                        &driver_store,
                                                        &truck_store,
                                                        &position_store,
                                                        &time_store,
                                                    ).await {
                                                        info!("Report created and sent successfully");
                                                    }
                                                    // let mut start_time_lock = start_time_store.lock().await;
                                                    // start_time_lock.insert(time_reg.driver_id.clone(), time_reg.clone());
                                                    // info!("Inserted into Start time store");
                                                }
                                        
                                                // {
                                                //     let start_time_snapshot = start_time_store.lock().await;
                                                //     info!("Updated Start time store: {:?}", *start_time_snapshot);
                                                // }
                                        
                                                // if !check_required_data(
                                                //     &time_reg.driver_id,
                                                //     &time_reg.truck_id,
                                                //     &driver_store,
                                                //     &truck_store,
                                                //     &start_position_store,
                                                //     &start_time_store,
                                                // )
                                                // .await
                                                // {
                                                //     info!(
                                                //         "Waiting for required data for driver {} and truck {}",
                                                //         time_reg.driver_id, time_reg.truck_id
                                                //     );
                                                //     continue; 
                                                // } else {
                                                //     sleep(Duration::from_millis(100)).await;
                                        
                                                //     info!(
                                                //         "Required data for driver {} and truck {} is available",
                                                //         time_reg.driver_id, time_reg.truck_id
                                                //     );
                                        
                                                //     info!("Data available as follows:");
                                        
                                                //     {
                                                //         let driver_store_lock = driver_store.lock().await;
                                                //         let driver = driver_store_lock.get(&time_reg.driver_id).unwrap();
                                                //         info!("driver_id : {:?}", time_reg.driver_id.clone());
                                                //         info!("driver first name : {:?}", driver.first_name.clone());
                                                //         info!("driver last name : {:?}", driver.last_name.clone());
                                                //         info!("driver email : {:?}", driver.email.clone());
                                                //         info!("driver phone : {:?}", driver.phone.clone());
                                                //     }
                                        
                                                //     {
                                                //         let truck_store_lock = truck_store.lock().await;
                                                //         let truck = truck_store_lock.get(&time_reg.truck_id).unwrap();
                                                //         info!("truck_id : {:?}", time_reg.truck_id.clone());
                                                //         info!("truck immatriculation : {:?}", truck.immatriculation.clone());
                                                //     }
                                        
                                                //     // Access start time details
                                                //     {
                                                //         let start_time_lock = start_time_store.lock().await;
                                                //         info!(
                                                //             "start time : {:?}",
                                                //             start_time_lock.get(&time_reg.driver_id).unwrap().timestamp.clone()
                                                //         );
                                                //     }
                                        
                                                //     // Access other details (end time, rest time, positions)
                                                //     {
                                                //         let end_time_lock = end_time_store.lock().await;
                                                //         let rest_time_lock = rest_time_store.lock().await;
                                                //         let start_position_lock = start_position_store.lock().await;
                                                //         let end_position_lock = end_position_store.lock().await;
                                                //         let rest_position_lock = rest_position_store.lock().await;
                                        
                                                //         info!(
                                                //             "end time : {:?}",
                                                //             end_time_lock.get(&time_reg.driver_id).unwrap().timestamp.clone()
                                                //         );
                                                //         info!(
                                                //             "rest time : {:?}",
                                                //             rest_time_lock.get(&time_reg.driver_id).unwrap().timestamp.clone()
                                                //         );
                                                        
                                                //         info!(
                                                //             "start position latitude : {:?}",
                                                //             start_position_lock.get(&time_reg.truck_id).unwrap().latitude.clone()
                                                //         );
                                                //         info!(
                                                //             "start position longitude : {:?}",
                                                //             start_position_lock.get(&time_reg.truck_id).unwrap().longitude.clone()
                                                //         );
                                                //         info!(
                                                //             "start position timestamp : {:?}",
                                                //             start_position_lock.get(&time_reg.truck_id).unwrap().timestamp.clone()
                                                //         );
                                                //         info!(
                                                //             "end position latitude : {:?}",
                                                //             end_position_lock.get(&time_reg.truck_id).unwrap().latitude.clone()
                                                //         );
                                                //         info!(
                                                //             "end position longitude : {:?}",
                                                //             end_position_lock.get(&time_reg.truck_id).unwrap().longitude.clone()
                                                //         );
                                                //         info!(
                                                //             "end position timestamp : {:?}",
                                                //             end_position_lock.get(&time_reg.truck_id).unwrap().timestamp.clone()
                                                //         );
                                                //         info!(
                                                //             "rest position latitude : {:?}",
                                                //             rest_position_lock.get(&time_reg.truck_id).unwrap().latitude.clone()
                                                //         );
                                                //         info!(
                                                //             "rest position longitude : {:?}",
                                                //             rest_position_lock.get(&time_reg.truck_id).unwrap().longitude.clone()
                                                //         );
                                                //         info!(
                                                //             "rest position timestamp : {:?}",
                                                //             rest_position_lock.get(&time_reg.truck_id).unwrap().timestamp.clone()
                                                //         );
                                                //     }
                                        
                                                //    // Create a new report
                                                //     let driver = {
                                                //         let lock = driver_store.lock().await;
                                                //         lock.get(&time_reg.driver_id).cloned().unwrap()
                                                //     };
                                                //     let truck = {
                                                //         let lock = truck_store.lock().await;
                                                //         lock.get(&time_reg.truck_id).cloned().unwrap()
                                                //     };
                                                //     let start_time = {
                                                //         let lock = start_time_store.lock().await;
                                                //         lock.get(&time_reg.driver_id).cloned().unwrap()
                                                //     };
                                                //     let end_time = {
                                                //         let lock = end_time_store.lock().await;
                                                //         lock.get(&time_reg.driver_id).cloned().unwrap()
                                                //     };
                                                //     let rest_time = {
                                                //         let lock = rest_time_store.lock().await;
                                                //         lock.get(&time_reg.driver_id).cloned().unwrap()
                                                //     };
                                                //     let start_position = {
                                                //         let lock = start_position_store.lock().await;
                                                //         lock.get(&time_reg.truck_id).cloned().unwrap()
                                                //     };
                                                //     let end_position = {
                                                //         let lock = end_position_store.lock().await;
                                                //         lock.get(&time_reg.truck_id).cloned().unwrap()
                                                //     };
                                                //     let rest_position = {
                                                //         let lock = rest_position_store.lock().await;
                                                //         lock.get(&time_reg.truck_id).cloned().unwrap()
                                                //     };

                                                //     let report = Report::new(
                                                //         time_reg.driver_id.clone(),
                                                //         driver.first_name.clone(),
                                                //         driver.last_name.clone(),
                                                //         driver.email.clone(),
                                                //         driver.phone.clone(),
                                                //         time_reg.truck_id.clone(),
                                                //         truck.immatriculation.clone(),
                                                //         start_time.timestamp.clone(),
                                                //         end_time.timestamp.clone(),
                                                //         rest_time.timestamp.clone(),
                                                //         start_position.latitude,
                                                //         start_position.longitude,
                                                //         start_time.timestamp.clone(),
                                                //         end_position.latitude,
                                                //         end_position.longitude,
                                                //         end_time.timestamp.clone(),
                                                //         rest_position.latitude,
                                                //         rest_position.longitude,
                                                //         rest_time.timestamp.clone(),
                                                //     );

                                        
                                                //     info!("Created new Report: {:?}", report);
                                        
                                                //     // Update the shared store
                                                //     store.insert(key.clone(), report.clone());
                                                //     info!("Updated Report store: {:?}", store);
                                        
                                                //     // Print the report
                                                //     info!("Report: {:?}", report);

                                                //     // Start a thread to watch the shared_store
                                                //     let shared_store_clone = Arc::clone(&shared_store);
                                                //     tokio::spawn({
                                                //         let client_config = client_config.clone();  // Clone the configuration once outside of the loop
                                                //         async move {
                                                //             let mut previous_len = 0;
                                                //             loop {
                                                //                 let current_len = {
                                                //                     let lock = shared_store_clone.lock().await;
                                                //                     lock.len()
                                                //                 };
                                                    
                                                //                 if current_len > previous_len {
                                                //                     let new_reports: Vec<Report> = {
                                                //                         let lock = shared_store_clone.lock().await;
                                                //                         lock.values().cloned().collect()
                                                //                     };
                                                    
                                                //                     for report in new_reports.iter().skip(previous_len) {
                                                //                         // Push each new report to the topic "report_topic"
                                                //                         push_to_kafka_topic(
                                                //                             &client_config.create().expect("Producer creation failed"),
                                                //                             "report_topic",
                                                //                             &report.driver_id,
                                                //                             &report,
                                                //                         ).await.unwrap();
                                                //                     }
                                                    
                                                //                     previous_len = current_len;
                                                //                 }
                                                    
                                                //                 tokio::time::sleep(Duration::from_millis(100)).await;
                                                //             }
                                                //         }
                                                //     });
                                                    
                                                // }
                                            }
                                            "end" => {
                                                {
                                                    let mut time_store_lock = time_store.lock().await;
                                                    let full_time_registration = time_store_lock.entry(time_reg.driver_id.clone()).or_insert(FullTimeRegistration::new());
                                                    full_time_registration.end = Some(time_reg.clone());
                                                    info!("Updated Time store with END time");

                                                    if check_create_and_send_report(
                                                        client_config.clone(),
                                                        Some(&time_reg.driver_id),
                                                        None,
                                                        &driver_store,
                                                        &truck_store,
                                                        &position_store,
                                                        &time_store,
                                                    ).await {
                                                        info!("Report created and sent successfully");
                                                    }
                                                    // let mut end_time_lock = end_time_store.lock().await;
                                                    // end_time_lock.insert(time_reg.driver_id.clone(), time_reg.clone());
                                                    // info!("Updated End time store");
                                                }
                                            }
                                            "rest" => {
                                                {
                                                    let mut time_store_lock = time_store.lock().await;
                                                    let full_time_registration = time_store_lock.entry(time_reg.driver_id.clone()).or_insert(FullTimeRegistration::new());
                                                    full_time_registration.rest = Some(time_reg.clone());
                                                    info!("Updated Time store with REST time");

                                                    if check_create_and_send_report(
                                                        client_config.clone(),
                                                        Some(&time_reg.driver_id),
                                                        None,
                                                        &driver_store,
                                                        &truck_store,
                                                        &position_store,
                                                        &time_store,
                                                    ).await {
                                                        info!("Report created and sent successfully");
                                                    }
                                                    // let mut rest_time_lock = rest_time_store.lock().await;
                                                    // rest_time_lock.insert(time_reg.driver_id.clone(), time_reg.clone());
                                                    // info!("Updated Rest time store");
                                                }
                                            }
                                            _ => {}
                                        }
                                        ,
                                        Event::Position(ref position) => match position.type_.as_str() {
                                            "start" => {
                                                {
                                                    let mut position_store_lock = position_store.lock().await;
                                                    let full_position = position_store_lock.entry(position.truck_id.clone()).or_insert(FullPosition::new());
                                                    full_position.start = Some(position.clone());
                                                    info!("Updated Position store with START position");

                                                    if check_create_and_send_report(
                                                        client_config.clone(),
                                                        Some(&position.truck_id),
                                                        None,
                                                        &driver_store,
                                                        &truck_store,
                                                        &position_store,
                                                        &time_store,
                                                    ).await {
                                                        info!("Report created and sent successfully");
                                                    }
                                                }
                                            }
                                            "end" => {
                                                {
                                                    let mut position_store_lock = position_store.lock().await;
                                                    let full_position = position_store_lock.entry(position.truck_id.clone()).or_insert(FullPosition::new());
                                                    full_position.end = Some(position.clone());
                                                    info!("Updated Position store with END position");

                                                    if check_create_and_send_report(
                                                        client_config.clone(),
                                                        Some(&position.truck_id),
                                                        None,
                                                        &driver_store,
                                                        &truck_store,
                                                        &position_store,
                                                        &time_store,
                                                    ).await {
                                                        info!("Report created and sent successfully");
                                                    }
                                                }
                                            }
                                            "rest" => {
                                                {
                                                    let mut position_store_lock = position_store.lock().await;
                                                    let full_position = position_store_lock.entry(position.truck_id.clone()).or_insert(FullPosition::new());
                                                    full_position.rest = Some(position.clone());
                                                    info!("Updated Position store with REST position");

                                                    if check_create_and_send_report(
                                                        client_config.clone(),
                                                        Some(&position.truck_id),
                                                        None,
                                                        &driver_store,
                                                        &truck_store,
                                                        &position_store,
                                                        &time_store,
                                                    ).await {
                                                        info!("Report created and sent successfully");
                                                    }
                                                }
                                            }
                                            _ => {}
                                            // Insert the position into the start, end, and rest position stores
                                            // start_position_store.lock().await.insert(position.truck_id.clone(), position.clone());
                                            // let start_position_snapshot = start_position_store.lock().await;
                                            // info!("Updated Start position store: {:?}", *start_position_snapshot);

                                            // end_position_store.lock().await.insert(position.truck_id.clone(), position.clone());
                                            // let end_position_snapshot = end_position_store.lock().await;
                                            // info!("Updated End position store: {:?}", *end_position_snapshot);

                                            // rest_position_store.lock().await.insert(position.truck_id.clone(), position.clone());
                                            // let rest_position_snapshot = rest_position_store.lock().await;
                                            // info!("Updated Rest position store: {:?}", *rest_position_snapshot);
                                        },
                                        Event::Unknown => error!("Unknown event type: {:?}", parsed_message),
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
        .expect("Failed to listen for ctrl_c");
    info!("Shutting down Kafka consumer...");
}


async fn push_to_kafka_topic<T: Serialize>(
    producer: &FutureProducer,
    topic: &str,
    key: &str,
    message: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    // Serialize the message to JSON
    let payload = serde_json::to_string(message)?;
    
    // Send the message to Kafka
    let delivery_status = producer
        .send(
            FutureRecord::to(topic)
                .key(key)
                .payload(&payload),
            Duration::from_secs(0),
        )
        .await;

    match delivery_status {
        Ok(_) => {
            println!("Message sent successfully to topic {}", topic);
            Ok(())
        }
        Err((e, _)) => {
            eprintln!("Failed to send message: {}", e);
            Err(Box::new(e))
        }
    }
}