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

    // Subscribe to the "entity_topic" Kafka topic
    consumer
        .subscribe(&[&CONFIG.topic])
        .expect("Subscription to topic failed");

    // Start an infinite loop for polling messages from Kafka
    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                if let Some(payload) = message.detach().payload() {
                    // Deserialize the payload assuming it's UTF-8 encoded
                    //It will be different depending on the data format

                    // Log the deserialized person object


                    println!("Received message: {:?}", payload);
                }
            }
            Some(Err(err)) => {
                println!("Error: {:?}", err);
            }
            None => {
                // println!("No message received");
            }
        }
    }
}