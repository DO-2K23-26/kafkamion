use std::process::ExitCode;

use fake::{Dummy, Fake, Faker};
use fake::faker::name::en::{Name, LastName, FirstName};
use fake::faker::internet::en::FreeEmail;

use serde::Serialize;
use uuid::Uuid;

use super::EventSource;

pub struct DriverEvent {
}

#[derive(Debug, Clone, Dummy, Serialize)]
pub struct Driver {
    #[dummy(expr = "\"driver\".to_string()")]
    r#type: String,
    #[dummy(expr = "Uuid::new_v4().to_string()")]
    driver_id: String,
    #[dummy(faker = "FirstName()")]
    first_name: String,
    #[dummy(faker = "LastName()")]
    last_name: String,
    #[dummy(faker = "FreeEmail()")]
    email: String,
    #[dummy(faker = "Name()")]
    phone: String
}

impl DriverEvent {
    pub fn new() -> Self {
        Self{
        }
    }
}

impl EventSource for DriverEvent {
    fn generate(&self) -> String {
        let data: Driver = Faker.fake();
        serde_json::to_string(&data).unwrap()
    }
}
