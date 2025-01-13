use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};

pub struct KafkaClient {
    producer: FutureProducer,
}

impl KafkaClient {
    pub fn new(broker: &str) -> Self {
        Self {
            producer: ClientConfig::new()
                .set("bootstrap.servers", broker)
                .set("message.timeout.ms", "5000")
                .create()
                .expect("Producer creation error"),
        }
    }

    pub async fn publish(&self, topic: &str, payload: &str, key: &str) {
        self.producer
            .send(
                FutureRecord::to(topic).payload(payload).key(key),
                Timeout::Never,
            )
            .await
            .unwrap();
    }
}
