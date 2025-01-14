use chrono::prelude::*;
use fake::faker::name::en::{FirstName, LastName};
use fake::faker::phone_number::en::PhoneNumber;
use fake::{Dummy, Fake, Faker};
use serde::Serialize;
use std::process::ExitCode;
use uuid::Uuid;

pub struct DriverEvent {
    data: Vec<Message>,
}

#[derive(Serialize, Dummy, Debug)]
struct Message {
    #[dummy(expr = "Uuid::new_v4().to_string()")]
    driver_id: String,
    #[dummy(faker = "FirstName()")]
    first_name: String,
    #[dummy(faker = "LastName()")]
    last_name: String,
    #[dummy(expr = "Faker.fake::<String>() + \"@gmail.com\"")]
    email: String,
    #[dummy(faker = "PhoneNumber()")]
    phone: String,
    #[dummy(expr = "Uuid::new_v4().to_string()")]
    truck_id: String,
    #[dummy(expr = "Faker.fake::<u16>() % 1000 + 1000")]
    immatriculation: u16,
    #[dummy(expr = "Utc::now().to_rfc3339()")]
    start_time: String,
    #[dummy(expr = "Utc::now().to_rfc3339()")]
    end_time: String,
    #[dummy(expr = "Utc::now().to_rfc3339()")]
    rest_time: String,
    #[dummy(expr = "Faker.fake::<f64>() * 180.0 - 90.0")]
    latitude_start: f64,
    #[dummy(expr = "Faker.fake::<f64>() * 360.0 - 180.0")]
    longitude_start: f64,
    #[dummy(expr = "Utc::now().to_rfc3339()")]
    timestamp_start: String,
    #[dummy(expr = "Faker.fake::<f64>() * 180.0 - 90.0")]
    latitude_end: f64,
    #[dummy(expr = "Faker.fake::<f64>() * 360.0 - 180.0")]
    longitude_end: f64,
    #[dummy(expr = "Utc::now().to_rfc3339()")]
    timestamp_end: String,
    #[dummy(expr = "Faker.fake::<f64>() * 180.0 - 90.0")]
    latitude_rest: f64,
    #[dummy(expr = "Faker.fake::<f64>() * 360.0 - 180.0")]
    longitude_rest: f64,
    #[dummy(expr = "Utc::now().to_rfc3339()")]
    timestamp_rest: String,
}

impl DriverEvent {
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    fn generate(&mut self) {
        let message: Message = Faker.fake(); // Générez un message factice
        self.data.push(message);
    }

    fn saver(&self) -> Result<ExitCode, ExitCode> {
        for message in &self.data {
            println!("{:?}", message); // Affichez chaque message au format Debug
        }
        Ok(ExitCode::SUCCESS)
    }
}

fn main() {
    let mut driver_event = DriverEvent::new();

    // Génération de 10 messages factices
    for _ in 0..10 {
        driver_event.generate();
    }

    // Sauvegarde (affichage dans la console ici)
    driver_event.saver().unwrap();
}
