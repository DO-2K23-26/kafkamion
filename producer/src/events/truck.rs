use fake::faker::automotive::fr_fr::LicencePlate;
use fake::{Dummy, Fake, Faker};

use serde::Serialize;
use uuid::Uuid;

use super::EventSource;

pub struct TruckEvent {}

#[derive(Debug, Clone, Dummy, Serialize)]
pub struct Truck {
    #[dummy(expr = "\"truck\".to_string()")]
    pub r#type: String,
    #[dummy(expr = "Uuid::new_v4().to_string()")]
    pub truck_id: String,
    #[dummy(faker = "LicencePlate()")]
    pub immatriculation: String,
}

impl TruckEvent {
    pub fn new() -> Self {
        Self {}
    }
}

impl EventSource<Truck> for TruckEvent {
    fn generate(&self) -> (Vec<String>, Vec<Truck>) {
        let data: Truck = Faker.fake();
        (vec![serde_json::to_string(&data).unwrap()], vec![data])
    }
}
