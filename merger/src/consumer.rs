use crate::config::CONFIG;
use crate::models::Person;
use log::{debug, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};
use std::time::Duration;

pub fn consumer(client_config: ClientConfig) {
    //log the configuration
    info!("Configuration: {:#?}", client_config);
    // Create a new Kafka consumer using the provided client configuration
    let consumer: BaseConsumer = client_config.create().expect("Consumer creation failed");

    // Subscribe to the "time_registration_topic" Kafka topic
    consumer
        .subscribe(&[&CONFIG.topic])
        .expect("Subscription to topic failed");

    // Start an infinite loop for polling messages from Kafka
    loop {
        match consumer.poll(Duration::from_millis(10000)) {
            Some(Ok(message)) => {
                // If a message is received successfully, extract and process its payload
                if let Some(payload) = message.detach().payload() {
                    // Deserialize the payload assuming it's UTF-8 encoded
                    //It will be different depending on the data format
                    let person: Person =
                        serde_json::from_slice(payload).expect("Unable to deserialize data");

                    // Log the deserialized person object
                    info!("{:#?}", person);
                }
            }
            Some(Err(err)) => {
                // If an error occurs during message consumption, print the error
                eprintln!("Error: {:?}", err);
            }
             // Do a print statement here
            None => {
                // If no message is received, print a debug message
                println!("No message received");
            }
        }
    }
}