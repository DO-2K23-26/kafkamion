use std::time::Duration;

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use tracing::{error, info};

pub struct KafkaClient {
    producer: FutureProducer,
}

pub enum KafkaError {
    KafkaNotAvailable,
}

impl KafkaClient {
    pub async fn new(broker: &str) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", broker);
        client_config.set("message.timeout.ms", "5000");

        let mut retry = 5;

        while !is_kafka_available(&client_config) && retry > 0 {
            error!("Kafka is not available. Retrying...");
            retry -= 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        if retry == 0 {
            error!("Kafka is not available. Exiting...");
            return Err(KafkaError::KafkaNotAvailable);
        }

        Ok(Self {
            producer: client_config.create().unwrap(),
        })
    }

    pub async fn publish(&self, topic: &str, payload: &str, key: &str) {
        info!("sent {} to topic {}", payload, topic);
        self.producer
            .send(
                FutureRecord::to(topic).payload(payload).key(key),
                Timeout::Never,
            )
            .await
            .unwrap();
    }
}

/// Checks if Kafka is available by attempting to connect to the server.
/// Returns `true` if the connection is successful, otherwise `false`.
pub fn is_kafka_available(client_config: &ClientConfig) -> bool {
    match client_config.create::<BaseConsumer>() {
        Ok(consumer) => {
            if consumer
                .fetch_metadata(None, Duration::from_secs(3))
                .is_ok()
            {
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
