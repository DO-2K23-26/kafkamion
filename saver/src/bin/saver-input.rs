use chrono::prelude::*;
use fake::faker::name::en::{FirstName, LastName};
use fake::faker::phone_number::en::PhoneNumber;
use fake::{Dummy, Fake, Faker};
use serde::Serialize;
use std::io::Write;
use std::process::ExitCode;
use uuid::Uuid;
use std::fs::File;
use serde_json::to_string_pretty;

pub struct MessageEvent {
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
    #[dummy(expr = "Faker.fake::<String>()")]
    immatriculation: String,
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

impl MessageEvent {
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    fn generate(&mut self) {
        let message: Message = Faker.fake();
        self.data.push(message);
    }

    fn saver(&self) -> Result<ExitCode, ExitCode> {
        for message in &self.data {
            println!("{:?}", message);
        }
        Ok(ExitCode::SUCCESS)
    }
}

fn main() {
    let mut message_event = MessageEvent::new();

    for _ in 0..10 {
        message_event.generate();
    }

    // Save the messages to a JSON file
    let json_file = "messages.json";
    let json_data = to_string_pretty(&message_event.data).unwrap();
    let mut file = File::create(json_file).unwrap();
    let _ = file.write(json_data.as_bytes());

    println!("Messages saved to {}", json_file);
}
