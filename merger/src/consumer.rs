use crate::config::CONFIG;
use log::{error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Checks if Kafka is available by attempting to connect to the server.
/// Returns `true` if the connection is successful, otherwise `false`.
pub fn is_kafka_available(client_config: &ClientConfig) -> bool {
    match client_config.create::<BaseConsumer>() {
        Ok(consumer) => {
            if consumer.fetch_metadata(None, Duration::from_secs(3)).is_ok() {
                info!("Kafka is reachable !");
                true
            } else {
                error!("Kafka is unreachable !");
                false
            }
        }
        Err(err) => {
            error!("An error has occured while trying to connect to Kafka : {:?}", err);
            false
        }
    }
}

pub fn consumer(client_config: ClientConfig) {
    // Log the configuration
    info!("Configuration: {:#?}", client_config);

    if !is_kafka_available(&client_config) {
        error!("Kafka is not available. Exiting...");
        return;
    }

    // Shared resource for logging or data processing
    let shared_resource = Arc::new(Mutex::new(Vec::new()));

    // List of topics to subscribe to
    let topics = vec!["topic1", "topic2", "topic3"];

    // Spawn a thread for each topic
    for topic in &CONFIG.topics {
        let client_config = client_config.clone();
        let shared_resource = Arc::clone(&shared_resource);
        let topic = topic.clone(); // Cloner le sujet pour le thread
    
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
                            {
                                let mut resource = shared_resource.lock().unwrap();
                                resource.push(format!("Topic: {}, Message: {}", topic, payload_str));
                            }
                            println!("Received from {}: {}", topic, payload_str);
                        }
                    }
                    Some(Err(err)) => {
                        eprintln!("Error in topic {}: {:?}", topic, err);
                    }
                    None => {
                        println!("No message received from topic: {}", topic);
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
