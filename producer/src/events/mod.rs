use std::sync::Arc;

use tracing::info;

use crate::kafka::KafkaClient;

pub mod driver;
pub mod position;
pub mod time_registration;
pub mod truck;

pub trait EventSource<T> {
    fn generate(&self) -> (Vec<String>, Vec<T>);
}

pub async fn generate_and_publish<T: EventSource<U>, U>(
    generator: Arc<T>,
    client: Arc<KafkaClient>,
    count: i32,
    topic: &str,
    message_type: &str,
) -> Vec<U> {
    let mut data_entity = Vec::new();
    for _ in 0..count {
        let client = client.clone();
        let event_generator = generator.clone();
        let (data, ids) = event_generator.generate();
        info!("generated {:?}", data);
        for message in data {
            client.publish(topic, &message, message_type).await;
        }
        for data in ids {
            data_entity.push(data);
        }
    }
    data_entity
}

pub async fn generate_and_publish_as_group<T: EventSource<U>, U>(
    generator: Arc<T>,
    client: Arc<KafkaClient>,
    count: i32,
    topic: &str,
    message_type: &str,
) -> Vec<Vec<U>> {
    let mut data_entity = Vec::new();
    for _ in 0..count {
        let client = client.clone();
        let event_generator = generator.clone();
        let (data, ids) = event_generator.generate();
        info!("generated {:?}", data);
        for message in data {
            client.publish(topic, &message, message_type).await;
        }
        data_entity.push(ids);
    }
    data_entity
}
